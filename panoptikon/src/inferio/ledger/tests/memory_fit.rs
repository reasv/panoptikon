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

/// A load report without `allocated_at_load_mb` — an older worker — prices
/// nothing at all, exactly as a missing `reserved_at_load_mb` used to.
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

/// The margin ring holds one entry per distinct `units` too, and for a
/// sharper reason than the fit ring: `pool_margin_locked` reads the
/// largest-`units` entry, small batches carry a *lower* ratio, so a long
/// steady state regrowing the pool at one small size would otherwise evict
/// the ramp's largest sample and quietly under-price every later grant.
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

    // Then far more than `FIT_RING` windows regrowing the pool at the
    // smallest size. Undeduped these would be 200 entries at 64 units and
    // the 256-unit ratio would be gone.
    for _ in 0..200 {
        window(grew(64, 640, 704));
    }
    assert!(
        (margin() - 1.5).abs() < 1e-9,
        "the largest batch still prices the grant: {}",
        margin()
    );
}

/// A steady state at one batch size no longer degenerates the fit ring:
/// it holds one sample per distinct `units`, so 200 repeats refresh a
/// single entry instead of evicting every pair Theil-Sen needs.
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

/// The fit runs in allocated currency over `allocated_at_load`, with a
/// free intercept — and Theil–Sen shrugs off a
/// single wild outlier that would drag least squares badly.
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
    // The residual is a *median* absolute deviation, so it is robust for
    // the same reason the slope is: one contaminated sample is
    // contamination, not model error, and must not widen every margin.
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
    let fit = ledger.health()[0].workers[0]
        .fit
        .as_ref()
        .map(|fit| fit.slope_mb_per_unit)
        .expect("fitted");
    assert!((fit - 10.0).abs() < 1e-6, "slope {fit}");
    // The anchor is 48 units, so the ramp's exponent is at 4 (32 <= 48) and
    // its next step is 64 — under the ratchet ceiling of 96, and reserved at
    // 64 * 10 = 640, not the whole share.
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
