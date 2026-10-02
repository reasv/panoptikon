//! The cost fit and the pool margin it is priced with.
use super::*;

/// Warm-pool batches price: `max_memory_allocated` has no caching
/// hysteresis, so a steady state whose pool never moves still teaches the
/// fit and still advances the ratchet anchor.
#[test]
fn warm_pool_batches_reach_the_fit() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(500));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    // The pool is flat at 2000 throughout; allocated runs 510 … 560 over an
    // `allocated_at_load` of 500, i.e. 10 MiB per 8 units.
    let warm: Vec<BatchMeasurement> = (1..=6)
        .map(|k| BatchMeasurement {
            reserved_before_mb: Some(2000),
            peak_reserved_mb: Some(2000),
            allocated_before_mb: Some(500),
            peak_allocated_mb: Some(500 + 10 * k),
            ..measurement(k * 8, 0, 0)
        })
        .collect();
    handle.lock().unwrap().record_measurements(warm);
    clean_window(&admission);
    let worker = &ledger.health()[0].workers[0];
    let fit = worker
        .fit
        .as_ref()
        .expect("six warm batches are six samples");
    assert_eq!(fit.samples, 6);
    assert!((fit.slope_mb_per_unit - 1.25).abs() < 1e-9, "{fit:?}");
    assert_eq!(worker.max_units_measured, 48, "the ratchet followed them");
}

/// A load report without `allocated_at_load_mb`, from an older worker,
/// prices nothing at all.
#[test]
fn a_worker_that_reports_no_allocated_baseline_feeds_no_fit() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    handle
        .lock()
        .unwrap()
        .load
        .as_mut()
        .unwrap()
        .value
        .allocated_at_load_mb = None;
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    for units in [4, 8, 16] {
        measured_window(&handle, &admission, units);
    }
    let worker = &ledger.health()[0].workers[0];
    assert!(
        worker.fit.is_none(),
        "no baseline, so nothing to price over"
    );
    assert_eq!(worker.max_units_measured, 0, "and no ratchet advance");
}

/// A batch that grew the pool but allocated less than
/// [`POOL_MARGIN_MIN_DELTA_MB`] teaches no margin — at that size the ratio
/// is allocator block granularity — so the default stands.
#[test]
fn a_tiny_pool_growth_teaches_no_margin() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    // The pool grows to twice the allocated peak, but that peak is 16 MiB.
    for units in [4u64, 8, 16] {
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                reserved_before_mb: Some(0),
                peak_reserved_mb: Some(2 * units),
                allocated_before_mb: Some(0),
                peak_allocated_mb: Some(units),
                ..measurement(units, 0, 0)
            }]);
        clean_window(&admission);
    }
    let fit = ledger.health()[0].workers[0]
        .fit
        .as_ref()
        .expect("three samples fit")
        .pool_margin;
    assert!((fit - POOL_MARGIN_DEFAULT).abs() < 1e-9, "{fit}");
}

/// The margin is the reserved/allocated ratio of the pool-growing batch
/// with the **most** units — the regime grants are issued in — clamped to
/// [`POOL_MARGIN_MIN`]..[`pool_margin_max`], here CUDA's.
#[test]
fn the_pool_margin_is_learned_from_the_largest_batch_and_clamped() {
    let ledger = ledger(1_000_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 900_000, 0);
    let grew = |units: u64, allocated: u64, reserved: u64| BatchMeasurement {
        reserved_before_mb: Some(0),
        peak_reserved_mb: Some(reserved),
        allocated_before_mb: Some(0),
        peak_allocated_mb: Some(allocated),
        ..measurement(units, 0, 0)
    };
    let margin = || {
        ledger.health()[0].workers[0]
            .fit
            .as_ref()
            .expect("a fit")
            .pool_margin
    };
    let window = |batch| {
        handle.lock().unwrap().record_measurements(vec![batch]);
        clean_window(&admission);
    };

    // Three sub-threshold batches first, so a fit exists to read the
    // margin off; none of them is big enough to teach one.
    for units in [1u64, 2, 3] {
        window(grew(units, 4 * units, 8 * units));
    }
    assert!(
        (margin() - POOL_MARGIN_DEFAULT).abs() < 1e-9,
        "{}",
        margin()
    );

    window(grew(64, 128, 192));
    assert!((margin() - 1.5).abs() < 1e-9, "{}", margin());

    // A *smaller* batch with a ratio of its own does not displace it.
    window(grew(32, 96, 96));
    assert!((margin() - 1.5).abs() < 1e-9, "{}", margin());

    // A larger one does — and an absurd ratio is clamped, not believed.
    window(grew(128, 256, 4_096));
    assert!(
        (margin() - POOL_MARGIN_MAX_CUDA).abs() < 1e-9,
        "{}",
        margin()
    );
}

/// The margin ring holds one entry per distinct `units`, so a steady state
/// regrowing the pool at one small size cannot evict the largest-`units`
/// sample the margin is read from.
#[test]
fn a_steady_state_at_one_size_keeps_the_largest_batchs_margin() {
    let ledger = ledger(1_000_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 900_000, 0);
    let grew = |units: u64, allocated: u64, reserved: u64| BatchMeasurement {
        reserved_before_mb: Some(0),
        peak_reserved_mb: Some(reserved),
        allocated_before_mb: Some(0),
        peak_allocated_mb: Some(allocated),
        ..measurement(units, 0, 0)
    };
    let margin = || {
        ledger.health()[0].workers[0]
            .fit
            .as_ref()
            .expect("a fit")
            .pool_margin
    };
    let window = |batch| {
        handle.lock().unwrap().record_measurements(vec![batch]);
        clean_window(&admission);
    };

    // A ramp whose largest batch is the loosest: 1.1, 1.1, then 1.5.
    window(grew(64, 640, 704));
    window(grew(128, 1_280, 1_408));
    window(grew(256, 2_560, 3_840));
    assert!((margin() - 1.5).abs() < 1e-9, "{}", margin());

    // Then 200 windows regrowing the pool at the smallest size, which would
    // evict the 256-unit ratio if the ring kept duplicates.
    for _ in 0..200 {
        window(grew(64, 640, 704));
    }
    assert!(
        (margin() - 1.5).abs() < 1e-9,
        "the largest batch still prices the grant: {}",
        margin()
    );
}

/// A steady state at one batch size keeps one fit-ring sample per distinct
/// `units`: 200 repeats refresh one entry instead of evicting the pairs
/// Theil-Sen needs.
#[test]
fn a_steady_state_at_one_size_leaves_the_slope_intact() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    // Six ramp steps on a 10 MiB/unit line.
    for units in [4u64, 8, 16, 32, 64, 128] {
        measured_window(&handle, &admission, units);
    }
    let slope = || {
        ledger.health()[0].workers[0]
            .fit
            .as_ref()
            .expect("a fit")
            .slope_mb_per_unit
    };
    assert!((slope() - 10.0).abs() < 1e-9, "{}", slope());
    for _ in 0..200 {
        measured_window(&handle, &admission, 128);
    }
    assert_eq!(
        ledger.calibration_state("g/a", GPU).unwrap().samples.len(),
        6,
        "one ring entry per distinct size"
    );
    assert!((slope() - 10.0).abs() < 1e-9, "{}", slope());
}

/// The fit runs in allocated currency over `allocated_at_load`, with a free
/// intercept, and Theil-Sen ignores a single wild outlier.
#[test]
fn fit_is_robust_to_one_outlier() {
    // delta = 200 + 10 * units, exactly.
    let mut samples: Vec<FitSample> = (1..=6)
        .map(|k| FitSample {
            units: k * 10,
            delta_mb: 200 + 10 * k * 10,
        })
        .collect();
    let clean = robust_fit(&samples).expect("fits");
    assert!((clean.slope_mb_per_unit - 10.0).abs() < 1e-9, "{clean:?}");
    assert!((clean.intercept_mb - 200.0).abs() < 1e-6, "{clean:?}");
    assert!(clean.residual_mb < 1e-6);
    assert_eq!(clean.samples, 6);

    // One contaminated sample (another process allocated mid-batch).
    samples.push(FitSample {
        units: 35,
        delta_mb: 9_000,
    });
    let robust = robust_fit(&samples).expect("still fits");
    assert!(
        (robust.slope_mb_per_unit - 10.0).abs() < 1.0,
        "the median of pairwise slopes absorbs the outlier: {robust:?}"
    );
    // The residual is a median absolute deviation: one contaminated sample
    // must not widen every margin.
    assert!(
        robust.residual_mb < 1.0,
        "one outlier does not inflate the confidence number: {robust:?}"
    );
    // Genuine scatter does, which is what margin-widening is for.
    let noisy: Vec<FitSample> = (1..=8)
        .map(|k| FitSample {
            units: k * 10,
            delta_mb: 200 + 10 * k * 10 + if k.is_multiple_of(2) { 300 } else { 0 },
        })
        .collect();
    let scattered = robust_fit(&noisy).expect("fits");
    assert!(
        scattered.residual_mb > 50.0,
        "a systematically scattered series reports its scatter: {scattered:?}"
    );
}

/// Degenerate fit inputs yield no fit rather than a nonsense one.
#[test]
fn degenerate_fits_are_refused() {
    assert!(robust_fit(&[]).is_none(), "no samples");
    assert!(
        robust_fit(&[
            FitSample {
                units: 4,
                delta_mb: 100
            },
            FitSample {
                units: 8,
                delta_mb: 200
            },
        ])
        .is_none(),
        "below MIN_FIT_SAMPLES"
    );
    let flat: Vec<FitSample> = (0..5)
        .map(|_| FitSample {
            units: 8,
            delta_mb: 300,
        })
        .collect();
    assert!(
        robust_fit(&flat).is_none(),
        "zero variance in units: nothing observed about the slope"
    );
    let falling: Vec<FitSample> = (1..=5)
        .map(|k| FitSample {
            units: k * 10,
            delta_mb: 1000 - k * 10,
        })
        .collect();
    assert!(
        robust_fit(&falling).is_none(),
        "a non-positive slope cannot price admission"
    );
}

/// Once a fit exists the unit budget derives from the MB share via the slope, and
/// the MB reservation is what the batch will actually cost — not the whole share.
#[test]
fn post_fit_units_derive_from_mb_via_the_slope() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    // A clean linear series of priced batches: 10 MB per unit.
    let series: Vec<BatchMeasurement> = (1..=6u64)
        .map(|k| measurement(k * 8, 0, 10 * k * 8))
        .collect();
    handle.lock().unwrap().record_measurements(series);
    clean_window(&admission);
    ledger.set_knee_for_test("g/a", GPU, 64);
    let fit = ledger.health()[0].workers[0]
        .fit
        .as_ref()
        .map(|fit| fit.slope_mb_per_unit)
        .expect("fitted");
    assert!((fit - 10.0).abs() < 1e-6, "slope {fit}");
    // The anchor is 48 units and the working size 64, under the ratchet
    // ceiling of 96: reserved at 64 * 10 = 640, not the whole share.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 64);
    assert_eq!(token.grant().mb, 640);
    assert!(admission.fit_to_send().is_some());
    assert!(admission.fit_to_send().is_none(), "only when it changed");
}

/// A snapshot is "sent" when it is *read* for a frame, so a window that never
/// delivered its frame — or fell back to per-request retries, which carry no
/// snapshot — would otherwise leave the worker permanently one version behind.
#[test]
fn an_undelivered_fit_is_re_sent_on_the_next_window() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    let series: Vec<BatchMeasurement> = (1..=6u64)
        .map(|k| measurement(k * 8, 0, 10 * k * 8))
        .collect();
    handle.lock().unwrap().record_measurements(series);
    clean_window(&admission);

    // A window takes the snapshot and then dies before the frame lands.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let snapshot = admission.fit_to_send().expect("a fit exists");
    assert!(admission.fit_to_send().is_none(), "already attached");
    token.finish(WindowOutcome::Aborted);
    assert_eq!(
        admission.fit_to_send().map(|fit| fit.version),
        Some(snapshot.version),
        "the same snapshot rides the next window: delivery was in doubt"
    );

    // A clean response is the one outcome that settles it as delivered.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    token.finish(WindowOutcome::Responded { oom: None });
    assert!(admission.fit_to_send().is_none(), "delivered and unchanged");

    // A window that responded with an OOM went through the per-request
    // fallback, whose frames carry no snapshot — so it re-arms too.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    assert!(admission.fit_to_send().is_some());
}

/// A replica of `model` whose batches allocate `fixed_mb + per_unit_mb ×
/// units`, ramped from a seed of 2 to 32 units on a roomy card.
fn fitted_with_a_fixed_part(
    ledger: &Arc<VramLedger>,
    model: &str,
    fixed_mb: u64,
    per_unit_mb: u64,
) -> (TelemetryHandle, Admission) {
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker(model, item_cost(2), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    for _ in 0..5 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget;
        let batch = measurement(units, 0, fixed_mb + per_unit_mb * units);
        handle.lock().unwrap().record_measurements(vec![batch]);
        token.finish(WindowOutcome::Responded { oom: None });
        admission.earn_next_size();
    }
    (handle, admission)
}

/// A grant covers the fit's intercept as well as its slope: with 800 MiB a
/// batch and 82 MiB a unit, 2186 MiB of room holds 16 units, not the 26 that
/// 2186 / 82 gives. The worker is told the fixed part.
#[test]
fn a_grant_prices_what_a_batch_costs_whatever_its_size() {
    let ledger = ledger(100_000, no_margin());
    let (handle, admission) = fitted_with_a_fixed_part(&ledger, "g/a", 800, 82);
    let fit = ledger.calibration_state("g/a", GPU).unwrap().fit.unwrap();
    assert!((fit.slope_mb_per_unit - 82.0).abs() < 1e-9, "{fit:?}");
    assert!((fit.intercept_mb - 800.0).abs() < 1e-9, "{fit:?}");

    // Ramped 2 → 32: the next size is 64, with room to spare.
    let roomy = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let grant = *roomy.grant();
    assert_eq!((grant.unit_budget, grant.mb), (64, 800 + 64 * 82));
    assert_eq!((grant.fixed_mb, grant.squeezed), (800, false));
    drop(roomy);

    push_memory(&handle, 2186, 0);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 2186);
    let tight = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let grant = *tight.grant();
    assert_eq!((grant.unit_budget, grant.mb), (16, 800 + 16 * 82));
    assert_eq!((grant.fixed_mb, grant.squeezed), (800, true));
    drop(tight);

    // Less room than the fixed part: one unit, and all the room there is.
    // It is less than one unit costs, so running out of memory there
    // counts towards declaring the replica unable to run.
    push_memory(&handle, 700, 0);
    ledger.ingest_all_for_test();
    let mut verdict = None;
    for _ in 0..OOM_WINDOWS_AT_FLOOR {
        let floor = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let grant = *floor.grant();
        assert_eq!((grant.unit_budget, grant.mb, grant.fixed_mb), (1, 700, 700));
        verdict = floor.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Marker),
        });
    }
    assert!(
        verdict.is_some(),
        "700 MiB is less than the 882 one unit costs"
    );
}

/// Two fitted models asking at once are weighed by the price of their
/// batches, fixed part included, and each one's floor is the price of its
/// seed batch.
#[test]
fn the_contention_split_counts_the_fixed_part() {
    let ledger = ledger(100_000, no_margin());
    // 32 units cost 1120 MiB with the fixed part and 320 without.
    let (handle, fixed) = fitted_with_a_fixed_part(&ledger, "g/fixed", 800, 10);
    let (_, plain) = fitted_with_a_fixed_part(&ledger, "g/plain", 0, 10);
    plain.note_demand(5);
    // (headroom, the units and MiB `g/fixed` is granted)
    for (headroom, granted) in [
        // 1120 / 1440 of the headroom.
        (1440, (32, 1120)),
        // 1120 / 1440 of 900 is 700: below the 820 its seed batch costs.
        (900, (2, 820)),
    ] {
        push_memory(&handle, headroom, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(GPU), headroom);
        let token = fixed.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!((token.grant().unit_budget, token.grant().mb), granted);
    }
}

/// A negative intercept prices as 0: a grant is never below `slope × units`.
#[test]
fn a_negative_intercept_is_not_priced() {
    let ledger = ledger(100_000, no_margin());
    let (_handle, admission) = fitted_with_a_fixed_part(&ledger, "g/a", 0, 10);
    let mut fit = ledger.calibration_state("g/a", GPU).unwrap().fit.unwrap();
    fit.intercept_mb = -300.0;
    ledger.install_fit_for_test("g/a", GPU, fit);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let grant = *token.grant();
    assert_eq!((grant.unit_budget, grant.mb, grant.fixed_mb), (64, 640, 0));
}

/// On the CPU device what a replica keeps of the fixed part stays resident
/// and is in its footprint, so a grant charges only the rest of it, times
/// the pool margin (1.25 here); a fixed part the replica hands back after
/// each batch is charged whole.
#[test]
fn the_cpu_device_charges_the_fixed_part_only_when_it_is_not_resident() {
    for (kept_mb, fixed_mb) in [(800, 0), (500, 375), (0, 1000)] {
        let ledger = cpu_ledger(no_margin());
        let handle = loaded_cpu(Some(CPU_RAM_MB));
        let admission = ledger
            .register_worker("g/a", item_cost(2), &handle, None)
            .expect("admitted");
        push_memory_with_total(&handle, 40_000, kept_mb, Some(CPU_RAM_MB), "ram");
        for _ in 0..5 {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let units = token.grant().unit_budget;
            let allocated = 800 + 82 * units;
            let batch = BatchMeasurement {
                rss_after_mb: Some(kept_mb),
                peak_reserved_mb: Some(allocated * 5 / 4),
                ..measurement(units, 0, allocated)
            };
            handle.lock().unwrap().record_measurements(vec![batch]);
            token.finish(WindowOutcome::Responded { oom: None });
            admission.earn_next_size();
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let grant = *token.grant();
        assert_eq!(
            (grant.unit_budget, grant.mb, grant.fixed_mb),
            (64, fixed_mb + 64 * 82 * 5 / 4, fixed_mb),
            "{kept_mb} MiB kept"
        );
    }
}

/// Ample headroom is room for one batch twice the appetite's size: the fixed
/// part counts once. With an anchor of 32 that is 800 + 64 × 10 MiB, not
/// twice the 1120 MiB of 32 units. Pre-fit it is twice the base.
#[test]
fn ample_headroom_is_the_price_of_the_doubled_batch() {
    let ample = |ledger: &Arc<VramLedger>, admission: &Admission| {
        let token = admission.request_grant(32, None, 1, 0).unwrap();
        assert!(!token.grant().squeezed);
        let state = ledger.lock();
        let grants = state
            .workers
            .values()
            .flat_map(|entry| entry.grants.values());
        grants
            .map(|charge| charge.ample_headroom)
            .collect::<Vec<_>>()
    };
    let fitted = ledger(100_000, no_margin());
    let (handle, admission) = fitted_with_a_fixed_part(&fitted, "g/a", 800, 10);
    for (room_mb, expected) in [(1440, true), (1439, false)] {
        push_memory(&handle, room_mb, 0);
        fitted.ingest_all_for_test();
        assert_eq!(ample(&fitted, &admission), [expected], "{room_mb} MiB");
    }
    // A card that affords the 32 units but not 64 has no room for 64: the
    // doubled batch is not cut to what the card affords. With no base of
    // its own, the replica's room is the card's whole limit.
    let small = ledger(100_000, no_margin());
    let (handle, admission) = fitted_with_a_fixed_part(&small, "g/a", 800, 10);
    small.lock().workers.values_mut().for_each(|entry| {
        entry.base_mb = None;
    });
    push_memory(&handle, 1300, 0);
    small.ingest_all_for_test();
    let limit = small.health()[0].limit_mb;
    assert_eq!((limit, small.headroom_mb(GPU)), (1300, 1300));
    assert_eq!(ample(&small, &admission), [false], "1300 MiB card");
    for (room_mb, expected) in [(2000, true), (1999, false)] {
        let cold = ledger(1000 + room_mb, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = cold
            .register_worker("g/a", item_cost(32), &handle, None)
            .unwrap();
        push_memory(&handle, room_mb, 0);
        cold.ingest_all_for_test();
        assert_eq!(
            ample(&cold, &admission),
            [expected],
            "pre-fit, {room_mb} MiB"
        );
    }
}
