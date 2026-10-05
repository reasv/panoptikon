//! A window's rate: which batches make it, and what marks it.
use super::*;

/// One clean window reporting warm-pool batches at the given rates.
fn warm_window(handle: &TelemetryHandle, admission: &Admission, batches: &[(u64, f64)]) {
    let window = batches.iter().map(|(units, _)| *units).max().unwrap_or(1);
    let token = admission
        .request_grant(window, None, 1, 0)
        .expect("granted");
    handle.lock().unwrap().record_measurements(
        batches
            .iter()
            .map(|(units, rate_)| warm_batch(*units, *rate_))
            .collect(),
    );
    token.finish(WindowOutcome::Responded { oom: None });
}

/// The last settled window's rate of `g/a`.
fn last_rate(ledger: &VramLedger) -> Option<WindowRate> {
    ledger.lock().calibration[&("g/a".to_owned(), GPU.to_owned())].last_rate
}

/// The warm-up: a replica's first settled window, and the batches it ran
/// before [`KNEE_WARMUP_BATCHES`], mark a window, so a one-batch first window
/// carries the mark into the next.
#[test]
fn a_replicas_first_windows_are_warm_up() {
    let warm_after = |first: &[(u64, f64)]| {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(2), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        warm_window(&handle, &admission, first);
        let first = last_rate(&ledger).expect("a rate").warmup;
        warm_window(&handle, &admission, &[(2, 3.0); 3]);
        (first, last_rate(&ledger).expect("a rate").warmup)
    };
    assert_eq!(warm_after(&[(2, 2.0)]), (true, true));
    assert_eq!(
        warm_after(&[(2, 2.0); WINDOW_DEPTH_MULTIPLIER as usize]),
        (true, false)
    );
}

/// Which measurements make a window's rate: priceable, non-negative,
/// unclamped full batches, and nothing else.
#[test]
fn only_clean_priceable_batches_make_the_windows_rate() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle.lock().unwrap().record_measurements(vec![
        // A pool-growing batch pays cudaMalloc for its size: kept.
        measurement(8, 0, 100),
        // An OOM and a WDDM spill: they measure the failure, not the curve.
        BatchMeasurement {
            oom: true,
            ..warm_batch(8, 500.0)
        },
        spilled_past_free(8, 10.0, 90_000),
        // Unpriceable: sub-batched inside `predict`, or no grant.
        BatchMeasurement {
            units: None,
            ..warm_batch(8, 500.0)
        },
        // No timing at all.
        BatchMeasurement {
            duration_ms: None,
            ..warm_batch(8, 500.0)
        },
        // No allocator reading: kept.
        BatchMeasurement {
            peak_reserved_mb: None,
            reserved_before_mb: None,
            ..warm_batch(8, 500.0)
        },
        // Half a reading is no reading.
        BatchMeasurement {
            reserved_before_mb: None,
            ..warm_batch(8, 500.0)
        },
        // Clamped by the worker: the size live free memory allowed, not
        // the model's choice.
        BatchMeasurement {
            clamped: Some(ClampReport {
                from_units: 8,
                to_units: 8,
                free_mb: Some(900),
                reason: None,
            }),
            ..warm_batch(8, 500.0)
        },
        // A warm one.
        warm_batch(8, 500.0),
    ]);
    token.finish(WindowOutcome::Responded { oom: None });

    assert_eq!(
        last_rate(&ledger).map(|rate| rate.units),
        Some(32),
        "five of the nine measurements are excluded, each for its own reason"
    );
}

/// **A batch cut short by a *shape* ceiling is excluded like one cut short by
/// memory**, though it arrives without a free reading: its size was not the
/// model's choice.
#[test]
fn an_index_limited_batch_is_excluded_from_the_knee_and_says_so() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle.lock().unwrap().record_measurements(vec![
        // The impl's shape ceiling, with no live free reading at hand.
        BatchMeasurement {
            clamped: Some(ClampReport {
                from_units: 8,
                to_units: 8,
                free_mb: None,
                reason: Some("index_limit".to_owned()),
            }),
            ..warm_batch(8, 500.0)
        },
        // The one that counts.
        warm_batch(8, 500.0),
    ]);
    token.finish(WindowOutcome::Responded { oom: None });

    assert_eq!(
        last_rate(&ledger).map(|rate| rate.units),
        Some(8),
        "an index-limited batch does not describe this model's curve, \
         whether or not it carried a free reading"
    );
}

/// The one state in which *every* batch of a window is disqualified from the
/// throughput curve, stated on the predicate itself.
#[test]
fn a_memory_blind_window_describes_no_throughput_curve() {
    let honest = GrantCharge {
        mb: 512,
        room: 512,
        requests: 1,
        unit_budget: 64,
        size_asked: 64,
        granted_at: Instant::now(),
        squeezed: false,
        room_bound: false,
        peak_occupants: 0,
        queue_bound: false,
        byte_bound: false,
        ram_mb: 0,
        ram_bound: false,
        ram_held: false,
        pressure: mps::MemoryPressure::Normal,
        item_cap: None,
    };
    assert!(ring_admits_window(&honest));
    assert!(
        ring_admits_window(&GrantCharge {
            squeezed: true,
            ..honest
        }),
        "a squeeze is the budget that card ran, and `unit_budget` is \
         already cut to it: the ramp earns a step off such a window, so \
         the ring may not refuse the same evidence"
    );
    assert!(
        !ring_admits_window(&GrantCharge { mb: 0, ..honest }),
        "a memory-blind grant priced nothing, so its rate describes nothing"
    );
    assert!(
        ring_admits_window(&GrantCharge {
            ram_bound: true,
            ..honest
        }),
        "host RAM cut it as the room does: its batches ran the size it set"
    );
    assert!(
        !ring_admits_window(&GrantCharge {
            pressure: mps::MemoryPressure::Warning,
            ..honest
        }),
        "the system was swapping, so its rate says nothing about the batch size"
    );
}

/// A squeezed window's batches reach the throughput ring at the size they
/// ran, and its pool-growing batch reaches the **cost fit**.
#[test]
fn a_squeezed_windows_batches_reach_the_fit_and_the_ring() {
    // 1 200 MiB of GPU against a 1 100 base: under `SEED_BATCH_FLOOR_MB` of
    // headroom, so squeezed.
    let ledger = ledger(1_200, no_margin());
    let handle = loaded(Some(1_100), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .unwrap();
    push_memory(&handle, 100, 0);

    let token = admission.request_grant(8, None, 1, 0).unwrap();
    assert!(token.grant().squeezed, "the fixture is the squeezed case");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(8, 500.0), measurement(8, 0, 40)]);
    token.finish(WindowOutcome::Responded { oom: None });

    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        last_rate(&ledger).map(|rate| rate.units),
        Some(16),
        "8 units is what this card could run, and the rate at 8 units is \
         what the batches measured"
    );
    assert_eq!(
        worker.max_units_measured, 8,
        "its batch is still an honest point on the memory curve"
    );
    assert_eq!(fit_sample_count(&ledger), 1);
}

/// One replica's warm windows, run while `neighbour` holds a window on the
/// same GPU for the whole of each of them.
fn contended_warm_window(
    handle: &TelemetryHandle,
    admission: &Admission,
    neighbour: &Admission,
    batches: &[(u64, f64)],
) {
    let window = batches.iter().map(|(units, _)| *units).max().unwrap_or(1);
    let held = neighbour.request_grant(4, None, 1, 0).expect("granted");
    let token = admission
        .request_grant(window, None, 1, 0)
        .expect("granted");
    assert!(!token.grant().squeezed, "the fixture is not a squeeze");
    handle.lock().unwrap().record_measurements(
        batches
            .iter()
            .map(|(units, rate_)| warm_batch(*units, *rate_))
            .collect(),
    );
    token.finish(WindowOutcome::Responded { oom: None });
    held.finish(WindowOutcome::Responded { oom: None });
}

/// A window run while a neighbour held one is marked contended.
#[test]
fn a_neighbours_overlapping_window_is_marked() {
    let ledger = priced_ledger(100_000);
    let handle = loaded(Some(1000), Some(0));
    let neighbour_handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    let neighbour = ledger
        .register_worker("g/b", item_cost(4), &neighbour_handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    for units in [8u64, 16, 32, 64] {
        contended_warm_window(
            &handle,
            &admission,
            &neighbour,
            &[
                (units, 100.0),
                (units, 100.0),
                (units, 100.0),
                (units, 100.0),
            ],
        );
    }

    assert!(
        last_rate(&ledger).expect("a rate").contended,
        "a window a neighbour overlapped is marked"
    );
}

/// The same windows with the GPU to itself carry no mark.
#[test]
fn the_same_windows_measured_alone_are_not_marked() {
    let ledger = priced_ledger(100_000);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    warm_window(&handle, &admission, &[(4, 100.0); 4]);
    assert!(!last_rate(&ledger).expect("a rate").contended);
}

/// End to end: a rate that rises to the seed's size and no further leaves
/// the working size there, it caps the grant, and it travels to the store as
/// local evidence.
#[test]
fn a_measured_working_size_caps_the_grant_and_is_persisted() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    let rate = |units: u64| units.min(64) as f64;
    let budgets: Vec<u64> = (0..40)
        .map(|_| window_at_the_rate(&handle, &admission, rate))
        .collect();
    assert!(budgets.iter().all(|units| *units <= 128), "{budgets:?}");
    // Between probes.
    while ledger.trial_for_test("g/a", GPU).0.is_some() {
        window_at_the_rate(&handle, &admission, rate);
    }

    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.knee_units, Some(64));
    assert!(worker.knee_is_local);
    assert_eq!(worker.unit_budget, 64);
    assert_eq!(
        admission.window_target_units(),
        64 * WINDOW_DEPTH_MULTIPLIER,
        "it caps the batch, not the window's depth in batches"
    );

    let last = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        (last.knee_units, last.max_units_measured),
        (Some(64), 128),
        "the size the probes left in place, beside the largest that ran"
    );

    // A settle that changes nothing writes nothing more.
    let written = profiles.updates.lock().unwrap().len();
    clean_window(&admission);
    assert_eq!(profiles.updates.lock().unwrap().len(), written);
}

/// A knee is a ceiling; deflation is a floor-ward correction.
#[test]
fn deflation_still_halves_below_the_knee() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: Some(16),
            knee_trials: Default::default(),
            sizes: Vec::new(),
            ram_ring: Vec::new(),
            ram_startup_mb: 0,
            ram_first_units: 0,
            local: false,
            fit_is_local: false,
            exact_torch: true,
            max_units_measured: 0,
            local_samples: 0,
            ring: Vec::new(),
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    ledger.ingest_all_for_test();

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        16,
        "a shipped knee may cap: it is a throughput hint, and capping is \
         the safe direction"
    );
    assert_eq!(
        token.grant().mb,
        200,
        "and the MB side follows the units, times the default pool margin"
    );
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        8,
        "deflation halves under the knee"
    );
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 4);
    drop(token);

    // Recovery is unaffected too.
    for _ in 0..(2 * CLEAN_WINDOWS_TO_RESTORE) {
        clean_window(&admission);
    }
    let knee = ledger.health()[0].workers[0]
        .knee_units
        .expect("still capped");
    assert!(knee >= 16, "the knee only ever widens: {knee}");
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        knee,
        "back to the knee, never past it"
    );
}

/// A seeded knee caps, but is never written back under our generator stamp.
#[test]
fn a_seeded_knee_is_never_laundered_into_local_provenance() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: Some(16),
            knee_trials: Default::default(),
            sizes: Vec::new(),
            ram_ring: Vec::new(),
            ram_startup_mb: 0,
            ram_first_units: 0,
            local: false,
            fit_is_local: false,
            exact_torch: true,
            max_units_measured: 0,
            local_samples: 0,
            ring: Vec::new(),
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(4, 0, 140)]);
    token.finish(WindowOutcome::Responded { oom: None });
    let update = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(update.max_units_measured, 4, "local evidence does travel");
    assert_eq!(
        update.knee_units, None,
        "but a knee this machine did not measure does not"
    );
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(16),
        "while still capping every window"
    );
    assert!(!ledger.health()[0].workers[0].knee_is_local);
}

/// The full round trip through the real store: a working size one run
/// earned is on disk, and the next run opens at it instead of the seed.
#[test]
fn a_persisted_knee_seeds_the_next_run() {
    let root = tempfile::tempdir().unwrap();
    let store = CalibrationStore::with_debounce(
        StorePaths {
            shipped_dirs: Vec::new(),
            local_path: root.path().join("inferio/calibration.toml"),
        },
        StoreEnv {
            platform: "windows".to_owned(),
            backend: "cuda".to_owned(),
            generator: "panoptikon test".to_owned(),
        },
        Duration::ZERO,
    );
    let ledger = VramLedger::for_test_with(
        &[(GPU, "TEST 9000", 100_000)],
        no_margin(),
        Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
    );
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    // The rate doubles with the batch up to 16 units and gains nothing past:
    // probes move the size to 16, and it is stored.
    for _ in 0..40 {
        window_at_the_rate(&handle, &admission, |units| units.min(16) as f64);
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(16));

    let seed = store
        .lookup(&item_query("g/a"))
        .expect("this run's own profile is on disk");
    assert_eq!(
        seed.knee_units,
        Some(16),
        "the working size round-trips through TOML"
    );

    // A fresh ledger over the same store: the next run.
    let next = VramLedger::for_test_with(
        &[(GPU, "TEST 9000", 100_000)],
        no_margin(),
        Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
    );
    let handle = loaded(Some(1000), Some(0));
    let admission = next
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    next.ingest_all_for_test();
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        16,
        "the next run's first window is the size the last one left in place"
    );
    assert!(
        next.health()[0].workers[0].knee_is_local,
        "a size this machine's own store holds was confirmed here"
    );
}

/// What makes a window's rate is decided by the window's own granted budget,
/// not by the batch's size in the abstract.
#[test]
fn only_budget_spending_batches_teach_the_knee() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(16), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    // Budget 16: a full batch is 13 units or more (0.8 x 16, rounded up).
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 16);
    handle.lock().unwrap().record_measurements(vec![
        warm_batch(16, 100.0),
        warm_batch(13, 96.0),
        // The window's tail: it ran small because the queue ran out.
        warm_batch(12, 90.0),
        warm_batch(1, 20.0),
    ]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        last_rate(&ledger).map(|rate| rate.units),
        Some(29),
        "the two batches that spent the budget, and neither tail"
    );

    // A user-capped window.
    let token = admission.request_grant(u64::MAX, Some(4), 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 16);
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(4, 95.0)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        last_rate(&ledger),
        None,
        "a capped batch says nothing about the size the model was free to run"
    );

    // A deflated grant: a batch that fills the small budget is full, and its
    // rate is honest data.
    admission
        .request_grant(u64::MAX, None, 1, 0)
        .unwrap()
        .finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 8, "halved by the deflation");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(8, 70.0)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        last_rate(&ledger).map(|rate| rate.units),
        Some(8),
        "a full batch on a deflated grant is admitted at its deflated size"
    );
}

/// A seed may prime a knee but never overwrite one this machine measured,
/// and a primed knee stays foreign.
#[test]
fn a_late_seed_never_overwrites_a_locally_fitted_knee() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    for _ in 0..5 {
        window_at_the_rate(&handle, &admission, |units| units.min(64) as f64);
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(64));
    assert!(ledger.health()[0].workers[0].knee_is_local);

    // Seeding again over live local state.
    let key = ("g/a".to_owned(), GPU.to_owned());
    {
        let mut state = ledger.lock();
        state.calibration.get_mut(&key).unwrap().seeded = false;
        VramLedger::seed_calibration_locked(
            &mut state,
            &key,
            true,
            Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: Some(1),
                knee_trials: Default::default(),
                sizes: Vec::new(),
                ram_ring: Vec::new(),
                ram_startup_mb: 0,
                ram_first_units: 0,
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: 0,
                ring: Vec::new(),
            }),
            "g/a",
            GPU,
        );
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.knee_units,
        Some(64),
        "a stranger's knee does not displace a measured one"
    );
    assert!(
        worker.knee_is_local,
        "and the local provenance survives the attempt"
    );

    // With no local knee to protect, the same seed is adopted — and stays
    // foreign, which is what keeps it out of the local store.
    let other = loaded(Some(1000), Some(0));
    let _second = ledger
        .register_worker("g/b", item_cost(64), &other, None)
        .unwrap();
    {
        let mut state = ledger.lock();
        let key = ("g/b".to_owned(), GPU.to_owned());
        VramLedger::seed_calibration_locked(
            &mut state,
            &key,
            true,
            Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: Some(16),
                knee_trials: Default::default(),
                sizes: Vec::new(),
                ram_ring: Vec::new(),
                ram_startup_mb: 0,
                ram_first_units: 0,
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: 0,
                ring: Vec::new(),
            }),
            "g/b",
            GPU,
        );
    }
    let health = ledger.health();
    let seeded = health[0]
        .workers
        .iter()
        .find(|worker| worker.inference_id == "g/b")
        .expect("registered");
    assert_eq!(seeded.knee_units, Some(16), "adopted where there was none");
    assert!(
        !seeded.knee_is_local,
        "and never laundered into local provenance"
    );
}

/// A knee-capped model claims a share sized `slope x min(anchor, knee)`, not
/// one for a batch it will never be admitted for.
#[test]
fn a_knee_shrinks_the_models_contention_appetite() {
    let ledger = ledger(10_000, no_margin());
    let a_handle = loaded(Some(1000), Some(0));
    let b_handle = loaded(Some(1000), Some(0));
    // Seed 1, so the contention floor (one seed batch) is 1000 MiB and
    // leaves the appetite split room to be the binding constraint.
    let a = ledger
        .register_worker("g/a", item_cost(1), &a_handle, None)
        .unwrap();
    let b = ledger
        .register_worker("g/b", item_cost(1), &b_handle, None)
        .unwrap();
    push_memory(&a_handle, 8000, 0);
    push_memory(&b_handle, 8000, 0);
    // Both fitted at 1000 MiB/unit, both with a ratchet anchor of 16:
    // identical appetites, so the headroom of 8000 splits evenly.
    for units in [4u64, 8, 16] {
        let window = |handle: &TelemetryHandle, admission: &Admission| {
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![measurement(units, 0, 1000 * units)]);
            token.finish(WindowOutcome::Responded { oom: None });
        };
        window(&a_handle, &a);
        window(&b_handle, &b);
    }
    a.earn_next_size();
    b.earn_next_size();
    assert_eq!(ledger.headroom_mb(GPU), 8000);

    a.note_demand(4);
    b.note_demand(4);
    let even = {
        let token = a.request_grant(u64::MAX, None, 4, 0).unwrap();
        let mb = token.grant().mb;
        drop(token);
        mb
    };
    assert_eq!(
        even, 4000,
        "half the headroom, and 4 units of the 1000 slope"
    );

    // A knee at 7 units: `a` can only use 7 of the 16 it has measured.
    ledger.set_knee_for_test("g/a", GPU, 7);
    a.note_demand(4);
    b.note_demand(4);
    let capped = {
        let token = a.request_grant(u64::MAX, None, 4, 0).unwrap();
        let mb = token.grant().mb;
        drop(token);
        mb
    };
    assert!(
        capped < even,
        "the appetite is now 7000 against b's 8000 — b's 16 units clamped to \
         the 8 this card affords (got {capped} against {even})"
    );
    assert_eq!(capped, 3000, "8000 × 7/15 = 3733 MiB, i.e. 3 whole units");
}

/// The smallest working size there is.
#[test]
fn a_working_size_of_one_still_grants_whole_units() {
    // A stored profile may carry `knee_units = 1`.
    let (ledger, _handle, admission) = knee_capped(1);
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.knee_units, Some(1), "the top of bucket 0 is 1");
    assert_eq!(worker.unit_budget, 1);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        1,
        "never zero: a batch is at least one item"
    );
    drop(token);
    assert_eq!(
        admission.window_target_units(),
        WINDOW_DEPTH_MULTIPLIER,
        "and the window is still several batches deep"
    );
}
