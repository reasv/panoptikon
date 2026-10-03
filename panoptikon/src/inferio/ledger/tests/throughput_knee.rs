//! The throughput ring: which batches feed it and which may decide a size.
use super::*;

fn curve(points: &[(u64, f64)], each: usize) -> VecDeque<ThroughputSample> {
    points
        .iter()
        .flat_map(|(units, rate_)| rate(*units, *rate_, each))
        .collect()
}

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

/// A slow small size, then a flat run of four larger ones, as windows.
fn bending_curve(handle: &TelemetryHandle, admission: &Admission) {
    for (units, rate_) in [
        (4u64, 40.0),
        (4, 40.0),
        (8, 100.0),
        (16, 100.0),
        (32, 100.0),
        (64, 100.0),
    ] {
        warm_window(handle, admission, &[(units, rate_); 4]);
    }
}

/// MiniLM's recorded observations of one size disagree by more than the
/// band, so they give no rate.
#[test]
fn minilms_recorded_size_is_refused_by_the_variance_filter() {
    // Two observations at `median x (1 ± d)` have relative MAD exactly `d`.
    let logged = 0.2128157093511856;
    let mut pair = [8950.0 * (1.0 - logged), 8950.0 * (1.0 + logged)];
    let dispersion = relative_mad(&mut pair).expect("finite positive median");
    assert!(
        (dispersion - logged).abs() < 1e-12,
        "the recorded dispersion: {dispersion}"
    );
    assert!(dispersion > KNEE_MAX_BUCKET_DISPERSION);
    let ring = recorded(&[(8, pair[0], 1), (8, pair[1], 1)]);
    assert_eq!(ring_rate(&ring, 8, KNEE_MAX_BUCKET_DISPERSION), None);
}

/// A batch counts as one of a size when it is a full batch of it
/// ([`FULL_BATCH_RATIO`]) or less than 1.11x larger: 52 to 71 units are
/// observations of 64, 51 and 72 are not.
#[test]
fn a_size_is_its_full_batches_and_those_a_little_larger() {
    let band = KNEE_MAX_BUCKET_DISPERSION;
    for (units, counted) in [(51, false), (52, true), (64, true), (71, true), (72, false)] {
        let ring = curve(&[(units, 100.0)], 3);
        assert_eq!(ring_rate(&ring, 64, band).is_some(), counted, "{units}");
    }
}

/// Observations taken beside another replica, or with a growing pool, are
/// read apart from the others: a size's rate is that of the conditions most
/// of the last six observations were taken in. A single one is no rate.
#[test]
fn a_rate_is_read_from_observations_in_the_same_conditions() {
    let band = KNEE_MAX_BUCKET_DISPERSION;
    let mut ring = curve(&[(8, 100.0)], 6);
    assert_eq!(ring_rate(&ring, 8, band), Some(100.0));
    assert_eq!(ring_rate(&ring, 16, band), None);
    let beside = ThroughputSample {
        units_per_sec: 50.0,
        occupants: 2,
        ..ring[0]
    };
    ring.extend([beside; 2]);
    assert_eq!(ring_rate(&ring, 8, band), Some(100.0), "two of six");
    ring.extend([beside; 2]);
    assert_eq!(ring_rate(&ring, 8, band), Some(50.0), "four of six");
    let grew = ThroughputSample {
        units_per_sec: 20.0,
        grew_pool: Some(true),
        ..ring[0]
    };
    ring.extend([grew; 4]);
    assert_eq!(ring_rate(&ring, 8, band), Some(20.0));
    let single = curve(&[(8, 100.0)], 1);
    assert_eq!(ring_rate(&single, 8, band), None);
}

/// The warm-up rule: a replica's first settled window is no measurement,
/// whatever the allocator says about its pool.
#[test]
fn the_replicas_first_window_is_no_measurement() {
    // A first window whose observations claim the model is three times
    // faster at 4 units than it ever is again.
    let series: Vec<Recorded> = [vec![(4, 300.0, 0); 3], vec![(4, 100.0, 1); 3]].concat();
    let ring = recorded(&series);
    assert_eq!(ring_rate(&ring, 4, KNEE_MAX_BUCKET_DISPERSION), Some(100.0));
    // Unmarked, they disagree with the honest ones by 0.5 and the size has
    // no rate at all.
    let unmarked: VecDeque<ThroughputSample> = ring
        .iter()
        .map(|sample| ThroughputSample {
            warmup: false,
            ..*sample
        })
        .collect();
    assert_eq!(ring_rate(&unmarked, 4, KNEE_MAX_BUCKET_DISPERSION), None);
}

/// When a replica's first window is a **single** batch, the runtime's
/// warm-up runs on into the next ones. [`KNEE_WARMUP_BATCHES`] carries the
/// mark on until the replica has run a window's worth of batches.
#[test]
fn a_first_window_of_one_batch_does_not_exhaust_the_warm_up() {
    // 0.943 s, 0.667 s and 0.490 s for two images each.
    const TAIL: [(u64, f64); 3] = [(2, 2.12), (2, 3.00), (2, 4.08)];
    let deciding_after = |first: &[(u64, f64)]| {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(2), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        warm_window(&handle, &admission, first);
        warm_window(&handle, &admission, &TAIL);
        ledger.deciding_samples_for_test("g/a", GPU)
    };
    assert_eq!(
        deciding_after(&[(2, 2.0)]),
        1,
        "after a one-batch first window the next two batches are warm-up too"
    );
    // The control: a first window run at depth spends the whole warm-up.
    assert_eq!(
        deciding_after(&[(2, 2.0); WINDOW_DEPTH_MULTIPLIER as usize]),
        3
    );
}

/// The bucket-variance band is per device kind: a quiet CPU host sits at
/// 0.13-0.20, an order of magnitude above the quiet GPU series
/// [`KNEE_MAX_BUCKET_DISPERSION`] was derived from.
#[test]
fn the_bucket_variance_band_is_the_devices_own() {
    // 0.30: past anything a quiet GPU shows, inside what a quiet CPU does.
    let noisy = curve(&[(8, 70.0), (8, 130.0)], 1);
    assert_eq!(
        ring_rate(&noisy, 8, KNEE_MAX_BUCKET_DISPERSION),
        None,
        "the accelerator band refuses it"
    );
    assert_eq!(
        ring_rate(&noisy, 8, super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION),
        Some(100.0),
        "the CPU device's band reads it"
    );
    // 0.025 of scatter is inside both.
    let quiet = curve(&[(8, 97.5), (8, 102.5)], 1);
    assert_eq!(
        ring_rate(&quiet, 8, KNEE_MAX_BUCKET_DISPERSION),
        Some(100.0)
    );
}

/// The band defaults to the device kind's, and a configured one follows the
/// inheritance rule of the rest of `[inference_local.vram]`.
#[test]
fn the_cpu_device_ships_its_own_band_and_a_user_overrides_it() {
    let cpu = crate::inferio::gpu::GpuInventory::known_cpu(CPU_RAM_MB);
    let card =
        crate::inferio::gpu::GpuInventory::known(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
    assert_eq!(
        with_shipped_gpu_defaults(&card, VramBudgets::default())
            .for_gpu("GPU-1a2b")
            .knee_dispersion_in_force(),
        KNEE_MAX_BUCKET_DISPERSION,
        "an accelerator keeps the band the GPU series produced"
    );
    assert_eq!(
        with_shipped_gpu_defaults(&cpu, VramBudgets::default())
            .for_gpu(super::cpu::DEVICE_KEY)
            .knee_dispersion_in_force(),
        super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION
    );

    let configured = with_shipped_gpu_defaults(
        &cpu,
        VramBudgets::default().with_gpu(
            super::cpu::DEVICE_KEY,
            VramBudget {
                knee_max_bucket_dispersion: Some(0.5),
                ..VramBudget::default()
            },
        ),
    );
    assert_eq!(
        configured
            .for_gpu(super::cpu::DEVICE_KEY)
            .knee_dispersion_in_force(),
        0.5,
        "a configured band wins, and the shipped cap_fraction still lands"
    );
    assert_eq!(
        configured.for_gpu(super::cpu::DEVICE_KEY).cap_fraction,
        Some(super::cpu::DEFAULT_CAP_FRACTION)
    );
}

/// The statistic itself, on the numbers its threshold was derived from.
#[test]
fn relative_mad_is_the_robust_dispersion_the_threshold_is_stated_in() {
    assert_eq!(relative_mad(&mut []), None);
    assert_eq!(
        relative_mad(&mut [0.0, 0.0]),
        None,
        "no scale to be relative to"
    );
    assert_eq!(relative_mad(&mut [100.0; 6]), Some(0.0));
    // A single factor-of-two outlier among five: a CV of 0.36 would refuse
    // the fit; the median-based statistic does not.
    let mut one_outlier = [100.0, 100.0, 100.0, 100.0, 100.0, 200.0];
    assert_eq!(relative_mad(&mut one_outlier), Some(0.0));
    // Half the samples off by a factor of two is disagreement: rejected.
    let mut disagreeing = [100.0, 100.0, 100.0, 200.0, 200.0, 200.0];
    let dispersion = relative_mad(&mut disagreeing).expect("finite positive median");
    assert!(
        dispersion > KNEE_MAX_BUCKET_DISPERSION,
        "{dispersion} must not pass the filter"
    );
}

/// Which measurements reach the throughput series: warm-pool, priceable,
/// non-negative ones and nothing else.
#[test]
fn only_clean_priceable_batches_reach_the_throughput_ring() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle.lock().unwrap().record_measurements(vec![
        // A pool-growing batch pays cudaMalloc for its size: kept, and
        // compared only with others that grew the pool.
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
        // No allocator reading: kept apart from the warm ones too.
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

    let pools: Vec<Option<bool>> = ledger.lock().calibration[&("g/a".to_owned(), GPU.to_owned())]
        .throughput
        .iter()
        .map(|sample| sample.grew_pool)
        .collect();
    assert_eq!(
        pools,
        [Some(true), None, None, Some(false)],
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
        ledger.health()[0].workers[0].throughput_samples,
        1,
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
        item_units: 1,
        items: 64,
        size_asked: 64,
        granted_at: Instant::now(),
        squeezed: false,
        room_bound: false,
        peak_occupants: 0,
        queue_bound: false,
        byte_bound: false,
        ram_mb: 0,
        ram_bound: false,
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
        worker.throughput_samples, 2,
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

/// Every observation taken while a neighbour held a window is kept and
/// tagged with it.
#[test]
fn a_neighbours_overlapping_window_is_tagged_on_every_observation() {
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

    let gpu = &ledger.health()[0];
    let worker = gpu
        .workers
        .iter()
        .find(|worker| worker.inference_id == "g/a")
        .expect("registered");
    assert_eq!(
        worker.throughput_samples, 16,
        "every observation is kept and tagged"
    );
    assert!(
        ledger.lock().calibration[&("g/a".to_owned(), GPU.to_owned())]
            .throughput
            .iter()
            .all(|sample| sample.occupants == 1)
    );
}

/// The same windows with the GPU to itself carry no tag.
#[test]
fn the_same_windows_measured_alone_do_decide() {
    let ledger = priced_ledger(100_000);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    bending_curve(&handle, &admission);
    assert_eq!(
        ledger.deciding_samples_for_test("g/a", GPU),
        20,
        "all but the first window's four"
    );
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
    let budgets: Vec<u64> = (0..6)
        .map(|_| window_at_the_rate(&handle, &admission, |units| units.min(64) as f64))
        .collect();
    assert_eq!(budgets, [64, 64, 128, 256, 32, 64]);

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
        (Some(64), 256),
        "the size a trial left in place, beside the largest that ran"
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
            knee_rates: Vec::new(),
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
            knee_rates: Vec::new(),
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
    // The rate doubles with the batch up to 16 units and gains nothing past.
    // The first trial moves the size to 16, and it is stored; the next
    // leaves it there.
    let budgets: Vec<u64> = (0..22)
        .map(|_| window_at_the_rate(&handle, &admission, |units| units.min(16) as f64))
        .collect();
    assert_eq!(budgets[..7], [4, 4, 8, 16, 32, 64, 16]);
    assert_eq!(budgets[18..], [32, 64, 8, 16]);
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

/// What reaches the throughput ring is decided by the window's own granted
/// budget, not by the batch's size in the abstract.
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
        ledger.health()[0].workers[0].throughput_samples,
        2,
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
        ledger.health()[0].workers[0].throughput_samples,
        2,
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
        ledger.health()[0].workers[0].throughput_samples,
        3,
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
                knee_rates: Vec::new(),
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
                knee_rates: Vec::new(),
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

/// On CUDA, a batch whose in-batch peak exceeds the pool it left is the
/// caching allocator freeing cached blocks to retry an allocation. The
/// post-batch pool reads it as **warm** and rings it, on any allocator.
#[test]
fn a_cuda_batch_that_released_cached_blocks_is_a_warm_ring_sample() {
    let (ledger, handle, admission) = ramping_from_seed(1);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let units = token.grant().unit_budget;
    // One batch: the pool peaked at 4 000 mid-batch and ended at 1 000,
    // exactly where it started. The peak says "grew", the after says "warm".
    let batch = BatchMeasurement {
        reserved_after_mb: Some(1_000),
        duration_ms: Some(units as f64 * 1000.0 / 20.0),
        ..measurement(units, 1_000, 4_000)
    };
    handle.lock().unwrap().record_measurements(vec![batch]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].workers[0].throughput_samples,
        1,
        "the post-batch pool calls this batch warm; the peak called it \
         pool-growing and kept it out of the ring"
    );
}
