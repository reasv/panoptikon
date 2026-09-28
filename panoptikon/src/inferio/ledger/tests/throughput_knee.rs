use super::*;

/// The expiry counter asks for **room**, not headroom. On the very shape
/// the credit exists for — a card whose limit its own pool has passed —
/// the saturated headroom is 0 for ever, so pricing `ample_headroom`
/// against it would make the knee a cap on exactly the card the credit was
/// written for. Priced against `share.room` the windows count and the knee
/// widens on schedule.
#[test]
fn a_knee_expires_on_the_card_whose_room_is_the_requesters_own_pool() {
    let ledger = ledger(200_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 190_000, 1000);
    measured_window(&handle, &admission, 64);
    ledger.set_knee_for_test("g/a", GPU, 15);

    // The pool now fills the card: free 0, footprint 191 000 of a 191 000
    // MiB limit. The grant stays wide — the credit — and so does the room
    // the expiry reads, though the headroom is 0.
    push_memory(&handle, 0, 190_000);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].headroom_mb, 0);

    for _ in 0..(KNEE_EXPIRY_CLEAN_WINDOWS - 1) {
        assert_eq!(
            window_at_the_cap(&handle, &admission),
            15,
            "still running at the knee, with its own pool paying for it"
        );
    }
    assert_eq!(
        ledger.knee_expiry_for_test("g/a", GPU).0,
        KNEE_EXPIRY_CLEAN_WINDOWS - 1,
        "every window at the knee earns expiry credit here"
    );
    window_at_the_cap(&handle, &admission);
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(31),
        "and the knee widens one bucket, as it does on an empty card"
    );
}

/// F1's shape against the new rule: a model whose smallest measured sizes
/// are flat because a fixed per-batch cost dominates them. Every sample is
/// ramp-era — the ramp had not gone past the candidate when they were
/// taken — which is exactly what rule 4 refused and the plateau exception
/// now waives.
#[test]
fn a_ramp_era_flat_bottom_fits_a_knee_at_the_floor() {
    let ramp_era = |rates: &[(u64, f64)]| -> Vec<ThroughputSample> {
        let mut out = Vec::new();
        for (units, rate) in rates {
            for _ in 0..4 {
                out.push(ThroughputSample {
                    units: *units,
                    units_per_sec: *rate,
                    // The ramp is *at* this size: nothing larger has run.
                    occupants: 0,
                    anchor: *units,
                    seq: out.len() as u64,
                    warmup: false,
                    warmup_tail: false,
                });
            }
        }
        out
    };
    // 4/8/16 units at 100/95/92 items/s, in that order, during the ramp.
    let samples = ramp_era(&[(4, 100.0), (8, 95.0), (16, 92.0)]);
    assert_eq!(
        fit_knee(&samples, 0.0, 16, None, KNEE_MAX_BUCKET_DISPERSION)
            .and_then(|fit| fit.knee_units),
        Some(7),
        "F1's number, from ramp-era evidence only"
    );
}

/// The variance filter is the only thing between that fit and noise: one
/// bucket whose samples disagree by more than the knee's own decision band
/// refuses the whole fit.
#[test]
fn only_a_floor_bucket_half_of_whose_samples_scatter_refuses_the_plateau() {
    let with_floor = |floor: &[f64]| -> Option<u64> {
        let mut out: Vec<ThroughputSample> = Vec::new();
        for rate in floor {
            out.push(ThroughputSample {
                units: 4,
                units_per_sec: *rate,
                occupants: 0,
                anchor: 4,
                seq: out.len() as u64,
                warmup: false,
                warmup_tail: false,
            });
        }
        for (units, rate) in [(8u64, 95.0), (16, 92.0)] {
            for _ in 0..4 {
                out.push(ThroughputSample {
                    units,
                    units_per_sec: rate,
                    occupants: 0,
                    anchor: units,
                    seq: out.len() as u64,
                    warmup: false,
                    warmup_tail: false,
                });
            }
        }
        fit_knee(&out, 0.0, 16, None, KNEE_MAX_BUCKET_DISPERSION).and_then(|fit| fit.knee_units)
    };
    // One sample 30 % slow and one 30 % fast among five: the median
    // absolute deviation is 0, so the plateau is fitted anyway. This is
    // what the filter does *not* catch (the standard deviation here is
    // 21 % of the mean).
    assert_eq!(
        with_floor(&[70.0, 100.0, 100.0, 100.0, 130.0]),
        Some(7),
        "a scattered floor bucket still decides a permanent cap"
    );
    // Only a bucket where **half** the samples are more than
    // KNEE_MAX_BUCKET_DISPERSION off the median is refused.
    assert_eq!(with_floor(&[75.0, 75.0, 100.0, 125.0, 125.0]), None);
}

fn curve(points: &[(u64, f64)], each: usize) -> Vec<ThroughputSample> {
    points
        .iter()
        .flat_map(|(units, rate_)| rate(*units, *rate_, each))
        .collect()
}

/// A hand-built series as the ledger would have recorded it: numbered in
/// order, and taken by a model whose ramp has already reached the widest
/// size in the series — i.e. steady state, not the climb.
fn stamped(samples: &[ThroughputSample]) -> (Vec<ThroughputSample>, u64) {
    let anchor = samples.iter().map(|sample| sample.units).max().unwrap_or(0);
    let stamped = samples
        .iter()
        .enumerate()
        .map(|(index, sample)| ThroughputSample {
            seq: index as u64,
            anchor,
            ..*sample
        })
        .collect();
    (stamped, anchor)
}

/// [`fit_knee`] with no historical anchor and no expiry behind it,
/// reduced to the knee itself — what a first fit on a fresh ring sees.
fn knee_of(samples: &[ThroughputSample]) -> Option<u64> {
    knee_against(samples, 0.0)
}

/// The same, held to a historical peak ([`ModelCalibration::knee_best`]).
fn knee_against(samples: &[ThroughputSample], floor: f64) -> Option<u64> {
    fit_against(samples, floor).and_then(|fit| fit.knee_units)
}

fn fit_against(samples: &[ThroughputSample], floor: f64) -> Option<KneeFit> {
    let (samples, anchor) = stamped(samples);
    fit_knee(&samples, floor, anchor, None, KNEE_MAX_BUCKET_DISPERSION)
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

/// The canonical shape the knee exists to find, run through as windows: a slow
/// small size, then a flat run of four larger ones.
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

/// The knee estimator's gates and rules, each shown binding on a hand-built
/// curve, and each with the control that shows the same series answering
/// once the rule is satisfied. See [`fit_knee`].
#[test]
fn the_knee_estimator_answers_a_curve_by_its_rules() {
    // A bucket alternating 100 and 200 has median 150, MAD 50, relative
    // MAD 0.333 — past KNEE_MAX_BUCKET_DISPERSION; 100 and 120 gives
    // 0.0909, which is inside it.
    let mut noisy = curve(&[(2, 40.0), (4, 100.0), (16, 100.0)], 4);
    noisy.extend(curve(&[(8, 100.0), (8, 200.0)], 2));
    let mut mild = curve(&[(2, 40.0), (4, 100.0), (16, 100.0)], 4);
    mild.extend(curve(&[(8, 100.0), (8, 120.0)], 2));
    let mut with_singleton = curve(&[(4, 100.0), (8, 100.0)], 6);
    with_singleton.extend(curve(&[(16, 100.0)], 1));
    let mut honest = curve(&[(2, 40.0), (4, 100.0), (8, 100.0)], 4);
    honest.extend(curve(&[(16, 100.0)], 2));
    let mut established = curve(&[(4, 100.0), (8, 180.0), (16, 200.0), (32, 205.0)], 4);
    established.extend(curve(&[(64, 206.0)], 2));
    let mut gapped = curve(&[(4, 100.0)], 4);
    gapped.extend(curve(&[(16, 100.0), (32, 100.0), (64, 100.0)], 4));

    for (label, samples, expected) in [
        (
            "a flat curve knees at its floor: both doublings above it were \
             measured and neither gained anything, so growing past it \
             spends memory for no throughput",
            curve(&[(4, 100.0), (8, 100.0), (16, 100.0), (32, 100.0)], 4),
            Some(7),
        ),
        (
            "the doubling immediately above the floor was never measured, \
             so the flat stretch does not reach down to it",
            gapped,
            None,
        ),
        (
            "a curve still gaining above its floor is untouched: the floor \
             is not on the plateau at all",
            curve(&[(4, 100.0), (8, 200.0), (16, 205.0), (32, 206.0)], 4),
            Some(15),
        ),
        (
            "the same flat run with a genuinely slower bucket below it does \
             bend, and knees at the top of bucket 2 (units 4..=7)",
            curve(&[(2, 40.0), (4, 100.0), (8, 100.0), (16, 100.0)], 4),
            Some(7),
        ),
        (
            "a plateau knees at its start: bucket 4 (units 16..=31) is \
             already within KNEE_RATIO of the best",
            curve(
                &[
                    (4, 100.0),
                    (8, 180.0),
                    (16, 200.0),
                    (32, 205.0),
                    (64, 206.0),
                ],
                4,
            ),
            Some(31),
        ),
        (
            "one bucket above the candidate is one comparison, not a \
             plateau (KNEE_PLATEAU_BUCKETS)",
            curve(&[(4, 100.0), (8, 180.0), (16, 200.0), (32, 205.0)], 4),
            None,
        ),
        (
            "one more bucket of the same flat run, and the same candidate \
             answers",
            established,
            Some(31),
        ),
        (
            "the frontier guard: a curve still climbing where it was last \
             measured has no knee",
            curve(&[(4, 100.0), (8, 200.0), (16, 400.0), (32, 800.0)], 4),
            None,
        ),
        (
            "9 observations is under MIN_KNEE_SAMPLES",
            curve(&[(4, 100.0), (8, 100.0), (16, 100.0)], 3),
            None,
        ),
        (
            "16 observations across 2 buckets describe a point, not a curve \
             (MIN_KNEE_BUCKETS)",
            curve(&[(4, 100.0), (8, 100.0)], 8),
            None,
        ),
        (
            "a third bucket holding one observation does not make it three: \
             a bucket whose dispersion cannot be measured takes no part \
             (MIN_KNEE_BUCKET_SAMPLES)",
            with_singleton,
            None,
        ),
        (
            "the same third size measured twice does, on a curve that bends",
            honest,
            Some(7),
        ),
        (
            "one bucket that disagrees with itself refuses the whole fit \
             (the bucket-variance filter)",
            noisy,
            None,
        ),
        (
            "the same bucket inside the dispersion threshold lets the fit \
             proceed",
            mild,
            Some(7),
        ),
    ] {
        assert_eq!(knee_of(&samples), expected, "{label}");
    }
}

/// wd-vit's knee ring at the instant it fitted `knee_units = 3`, run2 leg
/// `S2-wdvit` (`tools/calibration-protocol/results/run2/S2-wdvit`,
/// 2026-09-04T13:06:33.270Z, `observations=14`).
const WDVIT_RING_AT_ITS_FIRST_KNEE: &[Recorded] = &[
    (2, 37.35, 2, 1),
    (2, 44.18, 2, 1),
    (4, 40.49, 4, 2),
    (4, 29.43, 4, 2),
    (8, 36.13, 8, 3),
    (8, 44.49, 8, 3),
    (16, 40.99, 16, 4),
    (64, 40.07, 64, 6),
    (64, 39.87, 64, 7),
    (64, 39.71, 128, 9),
    (64, 40.47, 136, 11),
    (64, 39.49, 136, 13),
    (136, 39.00, 136, 14),
    (64, 39.43, 136, 15),
];

/// The recorded wd-vit ring, replayed.
#[test]
fn wd_vits_recorded_ring_knees_at_its_floor_once_the_frontier_is_quiet() {
    let ring = recorded(WDVIT_RING_AT_ITS_FIRST_KNEE);
    assert_eq!(ring.len(), 14, "the log's own `observations=14`");

    // Rule 1 still refuses this ring outright.
    assert_eq!(
        fit_knee(&ring, 0.0, 136, None, KNEE_MAX_BUCKET_DISPERSION).and_then(|fit| fit.knee_units),
        None,
        "no knee: the frontier the ring actually reached (136 units) holds \
         one observation and cannot be certified quiet"
    );

    // With the frontier quiet, the plateau is the answer: 37-44 items/s at
    // 2 units and 39 at 136 is a model that gains nothing from the memory
    // the ramp would spend reaching 136.
    let mut quiet_frontier = ring.clone();
    quiet_frontier.push(ThroughputSample {
        units: 136,
        units_per_sec: 39.0,
        occupants: 0,
        seq: 14,
        anchor: 136,
        warmup: false,
        warmup_tail: false,
    });
    assert_eq!(
        fit_knee(&quiet_frontier, 0.0, 136, None, KNEE_MAX_BUCKET_DISPERSION)
            .and_then(|fit| fit.knee_units),
        Some(3),
        "the floor bucket (2..=3 units), both doublings above it flat"
    );

    // The ramp's own end state, and the shape rule 2's exception is for: a
    // candidate every observation of which dates from the window that
    // stepped past it, with the two doublings immediately above it measured,
    // contiguous and flat. That is what the stop leaves behind — those two
    // buckets are its last two windows — so rule 4 is waived there and the
    // bend at 4 units is a knee.
    let ramp_era: &[Recorded] = &[
        (2, 20.0, 2, 1),
        (2, 20.0, 2, 1),
        (2, 20.0, 2, 1),
        (4, 40.0, 4, 2),
        (4, 40.0, 4, 2),
        (4, 40.0, 4, 2),
        (8, 41.0, 8, 3),
        (8, 41.0, 8, 3),
        (8, 41.0, 8, 3),
        (16, 41.0, 136, 4),
        (16, 41.0, 136, 4),
        (16, 41.0, 136, 4),
        (64, 40.0, 136, 6),
        (64, 40.0, 136, 6),
        (136, 39.0, 136, 8),
        (136, 39.0, 136, 8),
    ];
    assert_eq!(
        fit_knee(
            &recorded(ramp_era),
            0.0,
            136,
            None,
            KNEE_MAX_BUCKET_DISPERSION
        )
        .and_then(|fit| fit.knee_units),
        Some(7),
        "the top of bucket 2 (4..=7 units), 40 against the 41 the two \
         doublings above it measured"
    );
}

/// A plateau knee is a brake, not a cap: the expiry widens it, the model
/// runs at the wider size, and what that probe measures decides whether the
/// floor is still the answer.
#[test]
fn the_widening_probe_lifts_a_plateau_knee_a_wider_window_disproves() {
    // The knee at the floor was fitted from the 4-unit bucket; everything
    // above it is the probe, taken after the widening's mark.
    let probe = |rates: &[(u64, f64)]| -> Option<u64> {
        let mut ring = curve(&[(4, 100.0)], 4);
        ring.extend(curve(rates, 4));
        let ring: Vec<ThroughputSample> = ring
            .iter()
            .enumerate()
            .map(|(index, sample)| ThroughputSample {
                seq: if index < 4 {
                    index as u64
                } else {
                    10 + index as u64
                },
                anchor: 32,
                ..*sample
            })
            .collect();
        fit_knee(
            &ring,
            0.0,
            32,
            Some(KneeWidening {
                bucket: 2,
                from_seq: 10,
            }),
            KNEE_MAX_BUCKET_DISPERSION,
        )
        .and_then(|fit| fit.knee_units)
    };

    assert_eq!(
        probe(&[(8, 100.0), (16, 100.0), (32, 100.0)]),
        Some(7),
        "the probe ran a doubling wider and measured the same rate, so the \
         plateau is re-confirmed at the floor"
    );
    assert_eq!(
        probe(&[(8, 300.0), (16, 600.0), (32, 1200.0)]),
        None,
        "the probe measured more than the tolerance at every wider size, \
         so the floor is off the plateau and growth resumes"
    );
}

/// Rule 4's gate is held up by the ring, not by the live anchor.
#[test]
fn a_halved_anchor_does_not_excuse_a_knee_from_the_ramp_era_rule() {
    // A bend at 16 units, whose only observations date from the window
    // that was itself the ramp's step past 16, and whose next doubling was
    // never run — the gap is what keeps rule 2's exception off it;
    // everything above it is steady state at an anchor of 128.
    let ramp_era: &[Recorded] = &[
        (8, 40.0, 8, 3),
        (8, 40.0, 8, 3),
        (8, 40.0, 8, 3),
        (16, 100.0, 16, 4),
        (16, 100.0, 16, 4),
        (16, 100.0, 16, 4),
        (64, 100.0, 128, 6),
        (64, 100.0, 128, 6),
        (64, 100.0, 128, 6),
        (128, 98.0, 128, 7),
        (128, 98.0, 128, 7),
        (128, 98.0, 128, 7),
    ];
    assert_eq!(
        fit_knee(
            &recorded(ramp_era),
            0.0,
            64,
            None,
            KNEE_MAX_BUCKET_DISPERSION
        )
        .and_then(|fit| fit.knee_units),
        None,
        "the control: with the anchor as measured, rule 4 refuses"
    );
    // Two unified-memory-device deaths later the live anchor reads 16 — the same
    // bucket as the candidate, which is what used to skip the gate.
    assert_eq!(
        fit_knee(
            &recorded(ramp_era),
            0.0,
            16,
            None,
            KNEE_MAX_BUCKET_DISPERSION
        )
        .and_then(|fit| fit.knee_units),
        None,
        "a halved anchor is not evidence that the ramp never went past 16"
    );
    // And the rule still lets an honest knee through at the same anchor:
    // the same curve with the 16-unit observations taken after the ramp
    // had reached 64 is a steady-state window that happened to be small.
    let steady: Vec<Recorded> = ramp_era
        .iter()
        .map(|(units, rate_, anchor, window)| {
            (
                *units,
                *rate_,
                if *units == 16 { 64 } else { *anchor },
                *window,
            )
        })
        .collect();
    assert_eq!(
        fit_knee(
            &recorded(&steady),
            0.0,
            16,
            None,
            KNEE_MAX_BUCKET_DISPERSION
        )
        .and_then(|fit| fit.knee_units),
        Some(31),
        "honest evidence at 16 units still knees there"
    );
}

/// A veto refuses the fit; it never moves the knee up a bucket.
#[test]
fn a_vetoed_candidate_refuses_the_fit_rather_than_moving_up_a_bucket() {
    // A bend at 4 units and a plateau from 16 to 64, with the 4-unit
    // observations taken while the ramp was still stepping past 4, the
    // doubling above them never run, and everything else taken in steady
    // state at the anchor.
    let series: &[Recorded] = &[
        (2, 20.0, 2, 1),
        (2, 20.0, 2, 1),
        (2, 20.0, 2, 1),
        (4, 100.0, 4, 2),
        (4, 100.0, 4, 2),
        (4, 100.0, 4, 2),
        (16, 100.0, 64, 5),
        (16, 100.0, 64, 5),
        (16, 100.0, 64, 5),
        (32, 100.0, 64, 6),
        (32, 100.0, 64, 6),
        (32, 100.0, 64, 6),
        (64, 98.0, 64, 7),
        (64, 98.0, 64, 7),
        (64, 98.0, 64, 7),
    ];
    assert_eq!(
        fit_knee(&recorded(series), 0.0, 64, None, KNEE_MAX_BUCKET_DISPERSION)
            .and_then(|fit| fit.knee_units),
        None,
        "bucket 2 is the candidate and rule 4 refuses it, so there is no \
         knee — the fit does not go looking for a bucket that survives"
    );

    // The bucket an upward scan would have landed on, shown to be a
    // survivor so the assertion above is about the *shape* of the rules
    // and not about bucket 3 failing for some reason of its own: with the
    // ramp-era half of the ring replaced by steady-state observations at
    // the same rate, the candidate is bucket 2 again and it now passes,
    // which is the only difference between the two rings.
    let steady: Vec<Recorded> = series
        .iter()
        .map(|(units, rate_, _, window)| (*units, *rate_, 64, *window))
        .collect();
    assert_eq!(
        fit_knee(
            &recorded(&steady),
            0.0,
            64,
            None,
            KNEE_MAX_BUCKET_DISPERSION
        )
        .and_then(|fit| fit.knee_units),
        Some(7),
        "the same curve, honestly sampled, knees at the top of bucket 2"
    );
    // And bucket 4 really would have survived every rule on the original
    // ring, which is what makes the refusal a choice rather than a tie.
    let above_the_veto: Vec<Recorded> = series
        .iter()
        .filter(|(units, _, _, _)| *units != 4)
        .copied()
        .collect();
    assert_eq!(
        fit_knee(
            &recorded(&above_the_veto),
            0.0,
            64,
            None,
            KNEE_MAX_BUCKET_DISPERSION
        )
        .and_then(|fit| fit.knee_units),
        Some(31),
        "with the vetoed bucket gone the next one up is a legitimate knee"
    );
}

/// MobileCLIP's knee ring at the instant it fitted `knee_units = 127`, run2 leg
/// `S2-mobileclip`, 2026-09-04T13:11:26.964Z, `observations=15`.
const MOBILECLIP_RING_AT_ITS_KNEE: &[Recorded] = &[
    (2, 31.31, 2, 1),
    (2, 31.31, 2, 1),
    (4, 47.68, 4, 2),
    (4, 47.68, 4, 2),
    (8, 63.91, 8, 3),
    (8, 63.91, 8, 3),
    (16, 58.50, 16, 4),
    (64, 92.14, 64, 6),
    (64, 93.27, 64, 7),
    (64, 96.79, 128, 9),
    (64, 97.44, 136, 11),
    (64, 93.64, 136, 13),
    (136, 89.53, 136, 14),
    (64, 94.03, 136, 15),
    (136, 91.50, 136, 16),
];

/// The one-sided cost of [`KNEE_PLATEAU_BUCKETS`], stated in full.
#[test]
fn mobileclips_recorded_ring_knees_once_the_ramp_has_been_one_bucket_further() {
    let ring = recorded(MOBILECLIP_RING_AT_ITS_KNEE);
    assert_eq!(ring.len(), 15, "the log's own `observations=15`");
    assert_eq!(
        fit_knee(&ring, 0.0, 136, None, KNEE_MAX_BUCKET_DISPERSION).and_then(|fit| fit.knee_units),
        None,
        "one quiet bucket above the bend is one comparison, not a plateau"
    );

    // The same ring after two windows at 256 units, at the rate the 136s
    // were already running at: the plateau is now established across
    // buckets 7 and 8, and the knee is the one the leg fitted.
    let mut explored = MOBILECLIP_RING_AT_ITS_KNEE.to_vec();
    explored.push((256, 90.0, 272, 17));
    explored.push((256, 90.0, 272, 18));
    assert_eq!(
        fit_knee(
            &recorded(&explored),
            0.0,
            272,
            None,
            KNEE_MAX_BUCKET_DISPERSION
        )
        .and_then(|fit| fit.knee_units),
        Some(127),
        "the top of bucket 6 (units 64..=127), which is what the leg fitted"
    );
}

/// MiniLM, run2 leg `S2-minilm`: the variance filter refuses this model's only
/// multi-observation bucket, 59 times over the leg, and that is why it has no knee.
#[test]
fn minilms_recorded_bucket_is_refused_by_the_variance_filter() {
    // Two observations at `median × (1 ± d)` have relative MAD exactly
    // `d`, so the leg's logged figure reproduces from the figure itself.
    let logged = 0.2128157093511856;
    let mut pair = [8950.0 * (1.0 - logged), 8950.0 * (1.0 + logged)];
    let dispersion = relative_mad(&mut pair).expect("finite positive median");
    assert!(
        (dispersion - logged).abs() < 1e-12,
        "the dispersion the leg logged: {dispersion}"
    );
    assert!(dispersion > KNEE_MAX_BUCKET_DISPERSION);
}

/// Run1's `S6-contend`, the tainted series: three models sharing one GPU, and the
/// run1 binary fitted `knee_units` 15 / 31 / 16 383 out of it.
#[test]
fn a_contended_series_reaches_no_knee_at_all() {
    // The contention half: every observation carries a neighbour, so
    // `refit_knee_locked`'s filter hands the fit an empty ring.
    let contended: Vec<ThroughputSample> = recorded(MOBILECLIP_RING_AT_ITS_KNEE)
        .into_iter()
        .map(|sample| ThroughputSample {
            occupants: 2,
            ..sample
        })
        .collect();
    let sole: Vec<ThroughputSample> = contended
        .iter()
        .filter(|sample| sample.occupants == 0)
        .copied()
        .collect();
    assert!(sole.is_empty(), "nothing this series holds may fit a knee");
    assert_eq!(
        fit_knee(&sole, 0.0, 136, None, KNEE_MAX_BUCKET_DISPERSION),
        None
    );

    // The gate half: wd-vit's sole-occupancy census, in the proportions above and
    // scaled to what [`KNEE_RING`] can actually hold.
    let mut survivors = curve(&[(1, 36.0)], KNEE_RING - 5);
    survivors.extend(curve(&[(8, 36.0)], 4));
    survivors.extend(curve(&[(32, 36.0)], 1));
    assert_eq!(survivors.len(), KNEE_RING);
    assert_eq!(
        knee_of(&survivors),
        None,
        "a singleton at the frontier and two quiet buckets below it is \
         fewer buckets than a curve needs"
    );
}

/// The warm-up rule: a replica's first settled window contributes
/// no throughput observations, whatever the allocator says about its pool.
#[test]
fn the_replicas_first_window_teaches_the_knee_nothing() {
    // A bend at 4 units, and a first window at 4 units whose observations
    // claim the model is three times faster there than it ever is again.
    let mut series: Vec<Recorded> = vec![(4, 300.0, 32, 0), (4, 300.0, 32, 0), (4, 300.0, 32, 0)];
    for window in 1..=5u64 {
        let units = 1u64 << window;
        let rate_ = if units <= 2 { 40.0 } else { 100.0 };
        series.push((units, rate_, 32, window));
        series.push((units, rate_, 32, window));
        series.push((units, rate_, 32, window));
    }
    let ring = recorded(&series);
    assert_eq!(
        ring.iter().filter(|sample| sample.warmup).count(),
        3,
        "the first window's three observations are marked"
    );
    assert_eq!(
        fit_knee(&ring, 0.0, 32, None, KNEE_MAX_BUCKET_DISPERSION).and_then(|fit| fit.knee_units),
        Some(7),
        "the knee is the bend, not the warm-up window's fiction"
    );

    // The same series with the warm-up marks removed: the fiction becomes
    // the ring's best bucket and drags the threshold up with it.
    let unmarked: Vec<ThroughputSample> = ring
        .iter()
        .map(|sample| ThroughputSample {
            warmup: false,
            warmup_tail: false,
            ..*sample
        })
        .collect();
    assert_eq!(
        fit_knee(&unmarked, 0.0, 32, None, KNEE_MAX_BUCKET_DISPERSION)
            .and_then(|fit| fit.knee_units),
        None,
        "unmarked, the warm-up window's rates disagree with the same \
         bucket's honest ones by 0.5 and the variance filter refuses the \
         whole fit — a knee found late, and only because they were kept"
    );
}

/// N1 on the CPU device: the replica's first window is a **single**
/// 1-image batch, so the first window's mark alone leaves the runtime's
/// warm-up tail — the three 2-image batches straight after it, at relative
/// MAD 0.292 — standing in the ring as honest evidence, where one bucket
/// over the band refuses every fit for the rest of the job
/// (`results/final-n1`, leg n1-a: no knee at all, 256 units granted and
/// 8 387 MB of RSS against 1 890 MB on the two legs that kneed).
/// [`KNEE_WARMUP_BATCHES`] carries the mark on until the replica has run a
/// window's worth of batches.
#[test]
fn a_first_window_of_one_batch_does_not_exhaust_the_warm_up() {
    // The tail as the worker logged it: 0.943 s, 0.667 s and 0.490 s for
    // two images each.
    const TAIL: [(u64, f64); 3] = [(2, 2.12), (2, 3.00), (2, 4.08)];
    let plateau = [(8u64, 100.0), (16, 100.0), (32, 100.0), (64, 100.0)];

    let knee_after = |first: &[(u64, f64)]| {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(1), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        warm_window(&handle, &admission, first);
        warm_window(&handle, &admission, &TAIL);
        for (units, rate_) in plateau {
            warm_window(&handle, &admission, &[(units, rate_); 4]);
        }
        ledger.health()[0].workers[0].knee_units
    };

    let mut tail_rates = TAIL.iter().map(|(_, rate_)| *rate_).collect::<Vec<_>>();
    assert!(
        relative_mad(&mut tail_rates).unwrap() > KNEE_MAX_BUCKET_DISPERSION,
        "the tail is what the gate refused: {tail_rates:?}"
    );
    assert_eq!(
        knee_after(&[(1, 2.0)]),
        Some(15),
        "a one-batch first window is no warm-up either, so the tail is \
         marked too and the curve reads: the model is capped at the bend \
         instead of running free"
    );

    // And the control, which is every accelerator measured: a first window
    // that ran at depth spends the whole warm-up by itself, nothing after
    // it is marked, and the same tail refuses the fit exactly as it did
    // before this rule existed.
    assert_eq!(
        knee_after(&[(1, 2.0); WINDOW_DEPTH_MULTIPLIER as usize]),
        None,
        "the tail lands in the ring and one bucket over the band refuses \
         the whole fit"
    );
}

/// The bucket-variance band is per device kind. A quiet CPU host sits at
/// 0.13–0.20 in the buckets the ramp lives in, an order of magnitude above
/// the quiet GPU series [`KNEE_MAX_BUCKET_DISPERSION`] was derived from, so
/// the CPU device ships its own.
#[test]
fn the_bucket_variance_band_is_the_devices_own() {
    // 0.30: past anything a quiet GPU shows, inside what a quiet CPU does.
    let noisy = curve(&[(8, 70.0), (8, 130.0), (16, 100.0), (32, 100.0)], 1);
    let buckets = bucket_rates(&noisy, true);
    assert_eq!(
        quiet_medians(&buckets, KNEE_MAX_BUCKET_DISPERSION),
        None,
        "the accelerator band refuses it"
    );
    assert!(
        quiet_medians(&buckets, super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION).is_some(),
        "the CPU device's band reads it"
    );

    // A GPU-shaped ring is read the same way under either band.
    let quiet = curve(&[(8, 97.5), (8, 102.5), (16, 100.0), (32, 100.0)], 1);
    let quiet = bucket_rates(&quiet, true);
    assert_eq!(
        quiet_medians(&quiet, KNEE_MAX_BUCKET_DISPERSION),
        quiet_medians(&quiet, super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION),
        "0.05 of scatter is inside both"
    );
}

/// Where the band comes from: absent, it is the device kind's; configured,
/// it is the user's, on the same inheritance rule as the rest of
/// `[inference_local.vram]`.
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

/// A ring too noisy to summarize is **unknown**, not a gain. The two
/// callers of [`quiet_medians`] used to read the same refusal in opposite
/// directions — [`fit_knee`] installed nothing while [`ramp_still_gains`]
/// answered "free to grow", so noise released the brake and bought a
/// doubling a window (n1-a: 16 refusals, 1 -> 256 units).
#[test]
fn a_refused_fit_never_tells_the_ramp_it_still_gains() {
    let mut noisy = curve(&[(1, 40.0), (2, 60.0), (4, 100.0)], 2);
    noisy.extend(rate(8, 70.0, 1));
    noisy.extend(rate(8, 130.0, 1));
    let (noisy, anchor) = stamped(&noisy);
    assert!(
        bucket_rates(&noisy, false)
            .get(&size_bucket(anchor))
            .is_some_and(|rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES),
        "the frontier is measured; what it is not is quiet"
    );
    assert_eq!(
        fit_knee(&noisy, 0.0, anchor, None, KNEE_MAX_BUCKET_DISPERSION),
        None,
        "the fit is refused"
    );
    assert!(
        !ramp_still_gains(&noisy, anchor, 1, KNEE_MAX_BUCKET_DISPERSION),
        "and the ramp is told nothing, which is no growth"
    );
    assert!(
        ramp_still_gains(
            &noisy,
            anchor,
            1,
            super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION
        ),
        "the same ring under the band that can read it: 130 at 8 units \
         beats every bucket below, so the last doubling did buy something"
    );
}

/// A knee this process never measured is put on trial straight away
/// ([`KNEE_SEED_REVALIDATION_WINDOWS`]).
#[test]
fn a_seeded_knee_is_re_tested_sooner_than_one_this_run_measured() {
    let (ledger, handle, admission) = knee_capped(15);
    ledger.set_seeded_knee_for_test("g/a", GPU, 15);
    for window in 1..KNEE_SEED_REVALIDATION_WINDOWS {
        assert_eq!(window_at_the_cap(&handle, &admission), 15);
        assert_eq!(ledger.knee_expiry_for_test("g/a", GPU).0, window);
    }
    assert_eq!(window_at_the_cap(&handle, &admission), 15);
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(31),
        "four clean windows at a knee nothing in this run measured is all \
         the benefit of the doubt it gets"
    );
    // And it is sooner than a locally fitted knee's, which is the point.
    const _: () = assert!(KNEE_SEED_REVALIDATION_WINDOWS < KNEE_EXPIRY_CLEAN_WINDOWS);

    // Still provisional after the widening: nothing has yet made it this
    // run's measurement, so the next step is just as quick.
    assert!(!ledger.health()[0].workers[0].knee_is_local);
}

/// S3, replayed in miniature: a restarted run seeded with a stored knee must not
/// spend a whole job capped by a number it never re-validated.
#[test]
fn a_stored_knee_a_restart_never_re_validated_widens_until_it_is_withdrawn() {
    let (ledger, handle, admission) = knee_capped(7);
    ledger.set_seeded_knee_for_test("g/a", GPU, 7);
    // The ratchet anchor is 64 (`knee_capped`'s measured window), so the
    // knee stops binding once it reaches `RATCHET_FACTOR × 64`.
    let mut windows = 0;
    while ledger.health()[0].workers[0].knee_units.is_some() {
        // Every widening measures faster than the size before it, so no
        // refit puts the stranger's number back under rule 2's plateau.
        window_at_the_cap_rated(&handle, &admission, |granted| 10.0 * granted as f64);
        windows += 1;
        assert!(windows < 60, "the seeded knee never let go");
    }
    assert!(
        windows <= 6 * KNEE_SEED_REVALIDATION_WINDOWS as usize,
        "7 -> 15 -> 31 -> 63 -> 127, then withdrawn: {windows} windows"
    );
    assert_eq!(
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .unwrap()
            .grant()
            .unit_budget,
        128,
        "and the budget is the ramp's and the ratchet's again, not a \
         stranger's knee"
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
    // A single factor-of-two outlier among five honest samples: the CV
    // would be 0.36 and the fit would be refused; the median-based
    // statistic sees the outlier for what it is.
    let mut one_outlier = [100.0, 100.0, 100.0, 100.0, 100.0, 200.0];
    assert_eq!(relative_mad(&mut one_outlier), Some(0.0));
    // Half the samples off by a factor of two is not an outlier, it is
    // disagreement, and it is rejected.
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
fn only_clean_priceable_warm_batches_reach_the_knee_series() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle.lock().unwrap().record_measurements(vec![
        // A pool-growing batch: the cost fit's only input, and excluded
        // here — it pays cudaMalloc for the size it is reaching.
        measurement(8, 0, 100),
        // An OOM and a WDDM spill: they measure the failure, not the curve.
        BatchMeasurement {
            oom: true,
            ..warm_batch(8, 500.0)
        },
        spilled_past_free(8, 10.0, 90_000),
        // Unpriceable: the impl sub-batched inside `predict`, or the
        // request carried no grant at all.
        BatchMeasurement {
            units: None,
            ..warm_batch(8, 500.0)
        },
        // No timing at all.
        BatchMeasurement {
            duration_ms: None,
            ..warm_batch(8, 500.0)
        },
        // No allocator reading at all: a degraded host, where "the pool
        // did not grow" is an assumption rather than a measurement.
        BatchMeasurement {
            peak_reserved_mb: None,
            reserved_before_mb: None,
            ..warm_batch(8, 500.0)
        },
        // Half a reading is no reading either.
        BatchMeasurement {
            reserved_before_mb: None,
            ..warm_batch(8, 500.0)
        },
        // Clamped by the worker: the batch ran at the size live free memory
        // allowed, not at the size the model was free to reach (run2 R1a).
        BatchMeasurement {
            clamped: Some(ClampReport {
                from_units: 8,
                to_units: 8,
                free_mb: Some(900),
                reason: None,
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
        "eight of the nine measurements are excluded, each for its own reason"
    );
}

/// **S1: a batch cut short by a *shape* ceiling is excluded exactly like one cut
/// short by memory — and it arrives without a free reading.** Both clamps mean the
/// same thing to the knee ring: the size this batch ran at was not this model's
/// choice, so its rate says nothing about where the model's curve bends.
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

/// The window-wide half: the one state in which *every* batch of a window
/// is disqualified from describing the throughput curve, stated on the
/// predicate itself so the rule is readable without a GPU fixture.
#[test]
fn a_memory_blind_window_describes_no_throughput_curve() {
    let honest = GrantCharge {
        mb: 512,
        room: 512,
        requests: 1,
        unit_budget: 64,
        squeezed: false,
        peak_occupants: 0,
        knee_bound: false,
        ample_headroom: true,
        queue_bound: false,
        byte_bound: false,
    };
    assert!(knee_admits_window(&honest));
    assert!(
        knee_admits_window(&GrantCharge {
            squeezed: true,
            ..honest
        }),
        "a squeeze is the budget that card ran, and `unit_budget` is \
         already cut to it: the ramp earns a step off such a window, so \
         the ring may not refuse the same evidence"
    );
    assert!(
        !knee_admits_window(&GrantCharge { mb: 0, ..honest }),
        "a memory-blind grant priced nothing, so its rate describes nothing"
    );
}

/// The same rule end to end: a GPU with no headroom left squeezes the
/// window, and its warm batches reach the knee ring at the size they ran —
/// the one definition of "ran at its budget", the same one that lets the
/// ramp earn a step here. Its pool-growing batch reaches the **cost fit**,
/// which is a statement about memory and is true at whatever size ran.
#[test]
fn a_squeezed_windows_batches_reach_the_fit_and_the_knee() {
    // 1 200 MiB of GPU against a resident whose base is 1 100: under
    // `SEED_BATCH_FLOOR_MB` of headroom, which is what "squeezed" means pre-fit.
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
        worker.throughput_samples, 1,
        "8 units is what this card could run, and the rate at 8 units is \
         what the batch measured"
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

/// R1's contention tag: the very curve that fits a knee on a quiet GPU fits none at
/// all when a neighbour held a window across every one of its windows.
#[test]
fn a_neighbours_overlapping_window_keeps_a_curve_out_of_the_knee_fit() {
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
    assert_eq!(
        worker.knee_units, None,
        "none of them was measured with the GPU to itself"
    );
}

/// The same curve, sole occupancy, does knee — so the test above is about
/// the tag and not about the fixture.
#[test]
fn the_same_curve_measured_alone_does_knee() {
    let ledger = priced_ledger(100_000);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    bending_curve(&handle, &admission);
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
}

/// End to end: warm windows fit a knee, the knee caps the grant, and it
/// travels to the store as local evidence.
#[test]
fn a_fitted_knee_caps_the_grant_and_is_persisted() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    // One measured window, so the entry has local evidence to be
    // written with at all (the write policy's `local_samples > 0` guard).
    measured_window(&handle, &admission, 64);
    assert_eq!(ledger.health()[0].workers[0].knee_units, None);

    // A flat curve across four buckets: 16 observations, best at the
    // smallest, frontier well past it.
    bending_curve(&handle, &admission);

    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.knee_units, Some(15), "the top of bucket 3 (8..=15)");
    assert!(worker.knee_is_local);
    assert_eq!(worker.throughput_samples, 24);
    assert_eq!(
        worker.unit_budget, 15,
        "the knee caps the seed-and-anchor budget of 64"
    );

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 15);
    drop(token);

    assert_eq!(
        admission.window_target_units(),
        15 * WINDOW_DEPTH_MULTIPLIER,
        "the knee caps the batch, not the window's depth in batches"
    );

    let last = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        last.knee_units,
        Some(15),
        "a locally fitted knee is written"
    );

    // A settle that changes nothing writes nothing more: the knee is one
    // more evidence trigger, not a per-window write.
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
            local: false,
            fit_is_local: false,
            exact_torch: true,
            max_units_measured: 0,
            local_samples: 0,
            knee_clean_windows: 0,
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

/// A seeded knee caps, but is never written back out under our own generator stamp
/// — the same laundering rule the fit follows.
#[test]
fn a_seeded_knee_is_never_laundered_into_local_provenance() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: Some(16),
            local: false,
            fit_is_local: false,
            exact_torch: true,
            max_units_measured: 0,
            local_samples: 0,
            knee_clean_windows: 0,
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

    measured_window(&handle, &admission, 4);
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

/// The full round trip through the real store: a knee fitted in one run
/// is on disk, seeds the next one, and caps its very first window.
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
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    measured_window(&handle, &admission, 64);
    bending_curve(&handle, &admission);
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

    let seed = store
        .lookup(&item_query("g/a"))
        .expect("this run's own profile is on disk");
    assert_eq!(
        seed.knee_units,
        Some(15),
        "the knee round-trips through TOML"
    );

    // A fresh ledger over the same store: the next run.
    let next = VramLedger::for_test_with(
        &[(GPU, "TEST 9000", 100_000)],
        no_margin(),
        Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
    );
    let handle = loaded(Some(1000), Some(0));
    let admission = next
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    next.ingest_all_for_test();
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        15,
        "the seeded knee caps the first window of the next run"
    );
}

/// Log2 bucketing at its edges, including the two sizes a batch can never
/// actually be.
#[test]
fn size_buckets_are_defined_at_the_edges() {
    assert_eq!(
        size_bucket(0),
        0,
        "a zero-unit batch is impossible, and clamps rather than panicking \
         on ilog2(0)"
    );
    assert_eq!(size_bucket(1), 0, "the smallest real batch");
    assert_eq!(size_bucket(2), 1);
    assert_eq!(size_bucket(3), 1, "bucket 1 is 2..=3");
    assert_eq!(size_bucket(4), 2);
    assert_eq!(size_bucket(u64::MAX), 63, "and the top does not overflow");
}

/// What reaches the knee ring is decided by the window's own granted
/// budget, not by the batch's size in the abstract.
#[test]
fn only_budget_spending_batches_teach_the_knee() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(16), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    // Budget 16, so a full batch is 13 units or more (0.8 × 16 = 12.8,
    // rounded up: a batch is packed in whole items).
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

    // A deflated grant is the opposite case: the budget itself is small,
    // a batch that fills it *is* full, and how fast this model runs at
    // that size is honest data.
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

/// The descent this rule exists to prevent: once a knee caps the budget, every
/// window is a full batch at the cap plus tails below it.
#[test]
fn the_knee_does_not_ratchet_downward_under_its_own_cap() {
    let ledger = ledger(200_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(32), &handle, None)
        .unwrap();
    push_memory(&handle, 190_000, 1000);

    // A curve that climbs and then plateaus: bucket 3 (8..=15) is already within
    // 90% of the best, bucket 2 is not.
    for (units, rate_) in [(4u64, 80.0), (4, 80.0), (8, 95.0), (16, 99.0), (32, 100.0)] {
        warm_window(&handle, &admission, &[(units, rate_); 4]);
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 15);
    assert_eq!(
        ledger.knee_best_for_test("g/a", GPU),
        Some((5, 100.0)),
        "and the peak that defined it is remembered"
    );

    // Steady state under the cap, long enough that the ring (128) turns over and
    // the sizes above the knee age out of it entirely.
    let mut smallest_cap = u64::MAX;
    for _ in 0..120 {
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted = token.grant().unit_budget;
        smallest_cap = smallest_cap.min(granted);
        handle.lock().unwrap().record_measurements(vec![
            warm_batch(granted, 95.0),
            warm_batch(granted * 3 / 4, 92.0),
            warm_batch(granted / 2, 85.0),
            warm_batch(granted / 4, 70.0),
            warm_batch(1, 40.0),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
    }

    let worker = &ledger.health()[0].workers[0];
    assert!(
        worker.throughput_samples > 0,
        "each window's full-budget batch is admitted"
    );
    assert_eq!(
        smallest_cap, 15,
        "120 refits of a ring full of tails never capped below the fitted knee"
    );
    assert!(
        worker.knee_units.unwrap_or(u64::MAX) >= 15,
        "and the knee itself only ever moved outward: {:?}",
        worker.knee_units
    );
}

/// One clean window that spends its whole granted budget, whatever that
/// budget currently is.
fn window_at_the_cap(handle: &TelemetryHandle, admission: &Admission) -> u64 {
    window_at_the_cap_rated(handle, admission, |_| 100.0)
}

/// The same, with the window's rate a function of the budget it ran at —
/// what a model still gaining from every doubling looks like.
fn window_at_the_cap_rated(
    handle: &TelemetryHandle,
    admission: &Admission,
    rate_at: impl Fn(u64) -> f64,
) -> u64 {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(granted, rate_at(granted))]);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

/// A knee that has been right for [`KNEE_EXPIRY_CLEAN_WINDOWS`] clean windows,
/// on a GPU with room to spare, widens by one bucket.
#[test]
fn a_knee_expires_after_clean_windows_at_the_cap_with_room_to_spare() {
    let (ledger, handle, admission) = knee_capped(15);
    for window in 1..KNEE_EXPIRY_CLEAN_WINDOWS {
        assert_eq!(window_at_the_cap(&handle, &admission), 15);
        assert_eq!(
            ledger.knee_expiry_for_test("g/a", GPU).0,
            window,
            "one window of credit each"
        );
    }
    assert_eq!(window_at_the_cap(&handle, &admission), 15, "the last one");

    let (counter, re_explore) = ledger.knee_expiry_for_test("g/a", GPU);
    assert_eq!(counter, 0, "the counter resets with the widening");
    assert_eq!(
        re_explore,
        Some(3),
        "and the old cap's bucket is the frontier to be explored"
    );
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(31),
        "one log2 bucket wider — the ramp resumes one step above the knee, \
         not at whatever the ratchet would allow"
    );
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 31);
}

/// Both conditions, each shown to be load-bearing: a window that did not
/// run *at* the cap earns no credit, and neither does one on a GPU with
/// no room for the wider batch.
#[test]
fn only_a_window_run_at_the_cap_with_room_to_spare_counts_towards_expiry() {
    let (ledger, handle, admission) = knee_capped(15);

    // Short of work: the window asked for 4 units, so nothing about it
    // says the cap of 15 is still the right one.
    for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
        let token = admission.request_grant(4, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 4);
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(4, 100.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    assert_eq!(ledger.knee_expiry_for_test("g/a", GPU).0, 0);
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

    // A negative window resets whatever credit had accrued: a model that
    // just ran out of memory is not a model asking to be let out.
    window_at_the_cap(&handle, &admission);
    assert_eq!(ledger.knee_expiry_for_test("g/a", GPU).0, 1);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    assert_eq!(ledger.knee_expiry_for_test("g/a", GPU).0, 0);
}

/// A knee whose widening reaches the extrapolation ratchet's own ceiling
/// cannot cap anything any more, so it is withdrawn rather than left
/// standing as a number that does nothing.
#[test]
fn a_knee_widened_past_the_ratchet_ceiling_is_withdrawn() {
    // Anchor 64 ⇒ the ratchet allows 128, so a knee of 127 widens to 255
    // and stops binding.
    let (ledger, handle, admission) = knee_capped(127);
    for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
        window_at_the_cap(&handle, &admission);
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.knee_units, None, "withdrawn, not widened to 255");
    assert_eq!(worker.max_units_measured, 64);
    assert_eq!(worker.unit_budget, 128, "the ratchet governs from here");
    assert_eq!(
        ledger.knee_expiry_for_test("g/a", GPU).1,
        Some(size_bucket(127)),
        "a withdrawal is a widening with no upper bound, so it leaves the \
         same frontier for the ring to be let past"
    );
}

/// The other half of that guard, and the reason it is not merely tidy: the refit
/// runs **later in the very settle that withdraws the knee**, from a ring the
/// widenings never changed.
#[test]
fn a_withdrawn_knee_is_not_handed_straight_back_by_its_own_settle() {
    let (ledger, handle, admission) = knee_capped(127);
    for _ in 1..KNEE_EXPIRY_CLEAN_WINDOWS {
        window_at_the_cap(&handle, &admission);
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(127));

    // A ring a refit would read a knee of 15 out of, put in place with one window
    // of the expiry still to run.
    ledger.seed_throughput_ring_for_test("g/a", GPU, &[(8, 100.0), (16, 100.0), (32, 100.0)], 4);
    window_at_the_cap(&handle, &admission);

    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        None,
        "the knee stays withdrawn until the model has run above the cap it \
         was withdrawn from"
    );
    assert_eq!(
        ledger.knee_expiry_for_test("g/a", GPU).1,
        Some(size_bucket(127))
    );
}

/// The oscillation guard: right after a widening the ring is exactly what it was
/// when the knee expired, so a refit must not hand the same number straight back.
#[test]
fn a_widened_knee_is_not_refitted_until_the_model_has_run_wider() {
    let ledger = priced_ledger(200_000);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .unwrap();
    push_memory(&handle, 190_000, 1000);
    measured_window(&handle, &admission, 64);

    // A flat curve over four buckets fits a knee at the top of bucket 3.
    bending_curve(&handle, &admission);
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

    // Run it at the cap until it expires.
    let mut windows = 0;
    while ledger.health()[0].workers[0].knee_units == Some(15) {
        window_at_the_cap(&handle, &admission);
        windows += 1;
        assert!(
            windows <= KNEE_EXPIRY_CLEAN_WINDOWS,
            "the knee never expired"
        );
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(31));
    assert_eq!(
        ledger.knee_expiry_for_test("g/a", GPU).1,
        Some(3),
        "and the refit in that same settle did not restore it from the \
         ring the expiry just declared spent"
    );

    // One window at the wider size is the evidence the guard waits for:
    // [`MIN_KNEE_BUCKET_SAMPLES`] observations in the smallest quiet bucket above
    // the widened-from one, each with a sequence number past the widening's.
    assert_eq!(window_at_the_cap(&handle, &admission), 31);
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(31),
        "one observation above the old cap is not two: the guard asks for \
         a quiet bucket, and a bucket of one cannot be certified quiet"
    );
    assert_eq!(window_at_the_cap(&handle, &admission), 31);
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(15),
        "re-established from honest samples, which is what the expiry is for"
    );
    assert_eq!(
        ledger.knee_expiry_for_test("g/a", GPU).1,
        Some(3),
        "and the widening is still on the record: it is a sequence mark to \
         judge later evidence against, not a flag that gets consumed"
    );
}

/// The `anchor == 0` arm: a model that has never produced a local priced
/// sample has no ratchet ceiling, so `RATCHET_FACTOR × anchor` cannot say when a
/// widened knee has stopped mattering.
#[test]
fn a_knee_with_no_ratchet_anchor_is_withdrawn_once_it_stops_binding() {
    let ledger = priced_ledger(200_000);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .unwrap();
    push_memory(&handle, 190_000, 1000);
    ledger.set_knee_for_test("g/a", GPU, 3);
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        0,
        "nothing has been measured locally, so there is no ratchet ceiling"
    );

    // 3 → 7, still inside the seed-sized ramp's own ceiling of 8.
    for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
        window_at_the_cap(&handle, &admission);
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(7));

    // 15 would cap nothing the ramp allows, so the knee goes rather than
    // standing as a number nothing can act on.
    for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
        window_at_the_cap(&handle, &admission);
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, None);
}

/// The knee a run **seeded** is the one most in need of retiring — F-A's was
/// reseeded into 56 replicas — and it is not `knee_is_local`, so nothing the write
/// policy watches moves when it goes.
#[test]
fn a_withdrawn_seeded_knee_is_reported_to_the_store_as_a_withdrawal() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 1.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: Some(15),
            local: true,
            fit_is_local: true,
            exact_torch: true,
            max_units_measured: 64,
            local_samples: 20,
            knee_clean_windows: 0,
            ring: Vec::new(),
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 190_000, 1000);
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

    // 15 → 31 → 63 → withdrawn: three expiries against a ramp ceiling of 64, none
    // of which this replica ever wrote to the store, because a seeded knee is never
    // `knee_is_local`.
    for _ in 0..(KNEE_EXPIRY_CLEAN_WINDOWS * 3) {
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(1, 100.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, None);

    let updates = profiles.updates.lock().unwrap();
    let withdrawal = updates
        .iter()
        .find(|update| update.knee_withdrawn)
        .expect("the store is told, or the file keeps a retired knee forever");
    assert_eq!(
        withdrawal.knee_units, None,
        "and it carries no replacement, which is what the merge acts on"
    );
}

/// A persisted knee is reseeded **with its expiry state**, so a restart does not
/// hand it a fresh set of clean windows to be right in.
#[test]
fn a_seeded_knee_resumes_the_expiry_its_last_run_left() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 1.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: Some(15),
            local: true,
            fit_is_local: true,
            exact_torch: true,
            max_units_measured: 64,
            local_samples: 20,
            knee_clean_windows: KNEE_EXPIRY_CLEAN_WINDOWS - 1,
            ring: Vec::new(),
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 190_000, 1000);

    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
    assert_eq!(
        ledger.knee_expiry_for_test("g/a", GPU).0,
        KNEE_EXPIRY_CLEAN_WINDOWS - 1,
        "the counter came back with the knee"
    );
    assert_eq!(window_at_the_cap(&handle, &admission), 15);
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(31),
        "one window, not twelve, because eleven of them were paid last run"
    );
}

/// The other half of the same guarantee, on the fit itself: the threshold
/// is taken against the best this model has *ever* shown, not against
/// whatever survives in the ring.
#[test]
fn the_historical_peak_holds_the_knee_threshold_up() {
    // The ring a capped worker is left with: the peak has aged out and
    // what remains is a nearly flat run of sizes at and below the cap.
    let aged = curve(
        &[(2, 70.0), (4, 92.0), (8, 95.0), (16, 96.0), (32, 97.0)],
        3,
    );
    assert_eq!(
        knee_of(&aged),
        Some(7),
        "read on its own this ring knees two buckets lower"
    );
    assert_eq!(
        knee_against(&aged, 105.0),
        Some(15),
        "held to the peak the model actually reached, the plateau starts later"
    );
    assert_eq!(
        knee_against(&aged, 115.0),
        None,
        "and far enough below it, this ring describes no plateau at all"
    );
    assert_eq!(
        fit_against(&aged, 115.0).unwrap().best,
        (5, 97.0),
        "the ring's own best is reported either way, so the anchor can only rise"
    );
}

/// Which bucket carries the peak is not part of the answer: the threshold
/// is a rate, and the guard is on the knee bucket.
#[test]
fn a_noisy_plateau_knees_at_the_smallest_adequate_bucket() {
    // Five buckets, the four above the bend within ±5% of each other and the
    // maximum sitting in the middle of the range rather than at either end.
    let noisy = curve(
        &[(2, 40.0), (4, 98.0), (8, 100.0), (16, 102.0), (32, 99.0)],
        4,
    );
    assert_eq!(
        knee_of(&noisy),
        Some(7),
        "every bucket above the bend is within 90% of the best, so the \
         smallest of those wins"
    );

    // The ratio rule at its boundary, on the smallest bucket the rules
    // above allow to carry a knee.
    let at = curve(
        &[(2, 40.0), (4, 100.0 * KNEE_RATIO), (8, 100.0), (16, 100.0)],
        4,
    );
    assert_eq!(
        knee_of(&at),
        Some(7),
        "a bucket exactly at the ratio is on the plateau"
    );
    let under = curve(&[(2, 40.0), (4, 89.0), (8, 100.0), (16, 100.0)], 4);
    assert_eq!(
        knee_of(&under),
        None,
        "0.89 of the best is not, and the next bucket up has only the \
         frontier above it"
    );
    let mut wider = under;
    wider.extend(curve(&[(32, 100.0)], 2));
    assert_eq!(
        knee_of(&wider),
        Some(15),
        "one more quiet bucket above, and the knee is that next bucket up"
    );
}

/// A seed may prime a knee, never overwrite one this machine measured —
/// and a knee it does prime stays foreign, so it is never written back
/// out under our own generator stamp.
#[test]
fn a_late_seed_never_overwrites_a_locally_fitted_knee() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    bending_curve(&handle, &admission);
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
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
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: 0,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            "g/a",
            GPU,
        );
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.knee_units,
        Some(15),
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
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: 0,
                knee_clean_windows: 0,
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

/// A knee-capped model must not claim a share of the GPU sized for a batch it will
/// never be admitted for: the appetite is `slope × min(anchor, knee)`.
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

/// The smallest knee there is.
#[test]
fn a_knee_at_the_smallest_bucket_still_grants_whole_units() {
    // `knee_units = 1` is no longer reachable from a *fit* — a knee in the ring's
    // smallest bucket is refused outright (rule 2 of [`fit_knee`])
    // — but a shipped or stored profile may still carry one, and run1's F-A is
    // precisely a persisted `knee_units = 1`.
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

/// What travels to the store is the knee the ring **fitted**. The expiry's
/// widening is this process's re-test of that number, and persisting it
/// would start the next process at twice the cap this one learned — the
/// legs persisted 63 against a fit of 31.
#[test]
fn the_store_is_told_the_fitted_knee_not_the_one_the_expiry_widened_to() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(1), &handle, None)
        .expect("registers");
    push_memory(&handle, 190_000, 1000);
    let mut windows = 0;
    while ledger.health()[0].workers[0].knee_units.is_none() {
        ramp_window(&handle, &admission, &CLIP_M3_MAX);
        windows += 1;
        assert!(windows < 40, "the ramp never stopped");
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

    while ledger.health()[0].workers[0].knee_units == Some(15) {
        ramp_window(&handle, &admission, &CLIP_M3_MAX);
        windows += 1;
        assert!(windows < 80, "the knee never widened");
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(31));
    let updates = profiles.updates.lock().unwrap();
    assert_eq!(
        updates.last().expect("something was persisted").knee_units,
        Some(15),
        "the widened 31 is process state: its clean-window progress \
         travels, the cap it is probing with does not"
    );
}

/// The regression the CUDA warm rule would be if the brake were a bias: a
/// **GPU-bound** model, whose rate is still climbing at 256 units, is not
/// braked at a low rung by the ring filling earlier. MiniLM's ladder rises
/// through every rung on the card where wd-vit's flattens.
#[test]
fn a_gpu_bound_curve_is_not_braked_where_wd_vit_knees() {
    let (ledger, handle, admission) = ramping_from_seed(1);
    let mut budgets = Vec::new();
    for _ in 0..1_200 {
        budgets.push(mps_sampled_window(&handle, &admission, &MINILM_M3_MAX));
    }
    let held = *budgets.last().expect("windows");
    let worker = &ledger.health()[0].workers[0];
    assert!(
        held > 246,
        "a rising curve must not stop where wd-vit's flat one does; held \
         {held}, knee {:?}, first rungs {:?}",
        worker.knee_units,
        &budgets[..8]
    );
    // And the flat model on the identical harness is the contrast.
    let (flat_ledger, flat_handle, flat_admission) = ramping_from_seed(1);
    let mut flat = Vec::new();
    for _ in 0..1_200 {
        flat.push(mps_sampled_window(
            &flat_handle,
            &flat_admission,
            &WDVIT_M3_MAX,
        ));
    }
    assert!(
        *flat.last().expect("windows") < held,
        "wd-vit holds lower than MiniLM: {:?} vs {held}, knee {:?}",
        flat.last(),
        flat_ledger.health()[0].workers[0].knee_units
    );
}

/// The CUDA meaning change the post-batch pool rule carries, named: a batch
/// whose **in-batch peak** exceeds the pool it left. On CUDA that is the
/// caching allocator's own `release_cached_blocks` retry —
/// `memory_reserved()` falls when an allocation fails and torch frees
/// cached blocks to retry it, and `max_memory_reserved()` keeps the
/// pre-release figure. `4f2fd45c` read such a batch as pool-growing and
/// kept it out of the knee ring; the post-batch pool reads it as **warm**
/// and rings it, at the rate the retry stalled. Nothing about this is
/// gated on the Metal allocator.
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
