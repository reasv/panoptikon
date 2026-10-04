//! Pre-fit reservations beside other replicas: at most an equal part of the
//! headroom, at least the batch's price, and a batch cut to what its
//! reservation covers while a neighbour holds one.
use super::*;

/// A 16 GiB card with 15 333 MiB of headroom over two pre-fit models.
const CARD_MB: u64 = 16_368;
const CLIP_BASE_MB: u64 = 435;
const TAGS_BASE_MB: u64 = 600;
const HEADROOM_MB: u64 = CARD_MB - CLIP_BASE_MB - TAGS_BASE_MB;

/// What a seed batch is designed to grow the pool by: the seed budget times
/// the default pool margin.
const SEED_BATCH_MB: u64 = 2560;

fn pre_fit(ledger: &Arc<VramLedger>, model: &str, base_mb: u64, seed: u32) -> Admission {
    ledger
        .register_worker(
            model,
            item_cost(seed),
            &loaded(Some(base_mb), Some(0)),
            None,
        )
        .expect("registers")
}

fn window(admission: &Admission) -> GrantToken {
    admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted")
}

/// Two pre-fit models on one GPU: the first reserves half the headroom, the
/// second half of what is left, and both run their seed batch.
#[test]
fn two_pre_fit_models_each_reserve_a_part_and_run_their_seed_batch() {
    let ledger = ledger(CARD_MB, no_margin());
    let clip = pre_fit(&ledger, "g/clip", CLIP_BASE_MB, 8);
    let tags = pre_fit(&ledger, "g/tags", TAGS_BASE_MB, 4);
    ledger.record_free_for_test(GPU, HEADROOM_MB);
    assert_eq!(ledger.headroom_mb(GPU), 15_333);

    let first = window(&clip);
    assert_eq!(first.grant().mb, 7666, "15 333 / 2");
    assert_eq!(first.grant().unit_budget, 8);
    let second = window(&tags);
    assert_eq!(second.grant().mb, 3833, "(15 333 - 7666) / 2");
    assert_eq!(
        second.grant().unit_budget,
        4,
        "its seed batch, not one unit"
    );
    assert!(!second.grant().squeezed);
    assert!(ledger.take_pending_trims().is_empty());

    // Back-to-back windows: each asks while the other holds its part.
    drop(first);
    let first = window(&clip);
    assert_eq!(first.grant().mb, 5750, "(15 333 - 3833) / 2");
    drop(second);
    let second = window(&tags);
    assert_eq!(second.grant().mb, 4791, "(15 333 - 5750) / 2");
    assert_eq!(
        (first.grant().unit_budget, second.grant().unit_budget),
        (8, 4)
    );
}

/// A load in flight counts as a replica: a window granted while the second
/// model is still loading leaves it room for its first window.
#[tokio::test]
async fn a_window_granted_during_a_load_leaves_room_for_the_loading_model() {
    let ledger = ledger(CARD_MB, no_margin());
    let clip = pre_fit(&ledger, "g/clip", CLIP_BASE_MB, 8);
    ledger.record_free_for_test(GPU, CARD_MB - CLIP_BASE_MB);
    let loading = ledger
        .reserve_load_for_test("g/tags", item_cost(4), GPU, None)
        .await
        .expect("known GPU");
    let headroom = ledger.headroom_mb(GPU);
    assert_eq!(headroom, CARD_MB - CLIP_BASE_MB - CONSERVATIVE_BASE_MB);

    let first = window(&clip);
    assert_eq!(first.grant().mb, headroom / 2);
    drop(loading);
    let tags = pre_fit(&ledger, "g/tags", TAGS_BASE_MB, 4);
    let second = window(&tags);
    assert_eq!(second.grant().mb, (HEADROOM_MB - first.grant().mb) / 2);
    assert_eq!(second.grant().unit_budget, 4);
}

/// Three pre-fit replicas asking one after another: a third of what each
/// sees, or the seed batch's design cost when that is more, and headroom is
/// left over.
#[test]
fn three_pre_fit_replicas_each_reserve_a_third_of_what_they_see() {
    let ledger = ledger(18_000, no_margin());
    let replicas: Vec<Admission> = (0..3)
        .map(|index| pre_fit(&ledger, &format!("g/m{index}"), 1000, 4))
        .collect();
    ledger.record_free_for_test(GPU, 15_000);
    let tokens: Vec<GrantToken> = replicas.iter().map(window).collect();
    let reserved: Vec<u64> = tokens.iter().map(|token| token.grant().mb).collect();
    assert_eq!(reserved, vec![5000, 3333, SEED_BATCH_MB]);
    assert!(
        tokens
            .iter()
            .all(|token| token.grant().unit_budget == 4 && !token.grant().squeezed)
    );
    assert_eq!(ledger.headroom_mb(GPU), 15_000 - 10_893);
}

/// A pre-fit replica joining two fitted ones that are in a window reserves a
/// third of the headroom, so a fitted one can still grow its next window.
/// Only the pre-fit share is cut: the fitted one takes more than a third.
#[test]
fn a_pre_fit_replica_joining_busy_fitted_ones_leaves_them_room_to_grow() {
    let ledger = ledger(31_200, no_margin());
    // 400 MiB per unit, measured up to 24 units, which left a 9600 MiB pool.
    let fitted: Vec<(TelemetryHandle, Admission)> = (0..2)
        .map(|index| {
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker(&format!("g/fitted{index}"), item_cost(4), &handle, None)
                .expect("registers");
            let series = (1..=6u64).map(|k| measurement(k * 4, 0, 400 * k * 4));
            handle.lock().unwrap().record_measurements(series.collect());
            clean_window(&admission);
            ledger.set_knee_for_test(&format!("g/fitted{index}"), GPU, 32);
            (handle, admission)
        })
        .collect();
    let joining = pre_fit(&ledger, "g/joining", 1000, 4);
    // 3 bases and 2 pools are ours: 31 200 - 3000 - 19 200 is free.
    ledger.record_free_for_test(GPU, 9000);

    let busy: Vec<GrantToken> = fitted
        .iter()
        .map(|(_, admission)| admission.request_grant(24, None, 1, 0).expect("granted"))
        .collect();
    assert!(busy.iter().all(|token| token.grant().mb == 9600));
    assert_eq!(ledger.headroom_mb(GPU), 9000, "spent inside their pools");

    let token = window(&joining);
    assert_eq!(token.grant().mb, 3000, "9000 / 3");
    assert_eq!(token.grant().unit_budget, 4);
    assert!(!token.grant().squeezed);

    // The first fitted replica's next window is 8 units larger: its pool
    // and 3200 of the 6000 left, more than a third of it.
    let mut busy = busy.into_iter();
    drop(busy.next());
    let grown = window(&fitted[0].1);
    assert_eq!(grown.grant().unit_budget, 32);
    assert_eq!(grown.grant().mb, 12_800);
    assert!(!grown.grant().squeezed);
}

/// A fitted requester's share is not cut in the appetite split either: with
/// a hungry pre-fit neighbour it takes 7500 of 12 000, more than half.
#[test]
fn a_fitted_requester_is_not_held_to_an_equal_part_in_the_split() {
    let ledger = ledger(16_000, no_margin());
    let handle = loaded(Some(3000), Some(0));
    let fitted = ledger
        .register_worker("g/fitted", item_cost(4), &handle, None)
        .expect("registers");
    // 1500 MiB per unit, 1875 with the default pool margin.
    ledger.install_fit_for_test(
        "g/fitted",
        GPU,
        FitSnapshot {
            slope_mb_per_unit: 1500.0,
            intercept_mb: 0.0,
            residual_mb: 0.0,
            samples: 20,
            version: 1,
        },
    );
    let neighbour = pre_fit(&ledger, "g/neighbour", 1000, 4);
    ledger.record_free_for_test(GPU, 12_000);
    fitted.note_demand(1);
    neighbour.note_demand(1);

    let token = window(&fitted);
    assert_eq!(token.grant().unit_budget, 4);
    assert_eq!(token.grant().mb, 7500);
    assert!(!token.grant().squeezed);
}

/// Two of three replicas ask together: each share is cut to a third of the
/// headroom, not to half.
#[test]
fn the_split_is_cut_to_a_part_among_all_replicas_not_only_the_hungry() {
    let ledger = ledger(18_000, no_margin());
    let replicas: Vec<Admission> = (0..3)
        .map(|index| pre_fit(&ledger, &format!("g/m{index}"), 1000, 4))
        .collect();
    ledger.record_free_for_test(GPU, 15_000);
    replicas[0].note_demand(1);
    replicas[1].note_demand(1);
    assert_eq!(window(&replicas[0]).grant().mb, 5000, "not 15 000 / 2");
}

/// A stored fit with a non-positive slope prices nothing, so its replica is
/// pre-fit: beside a neighbour it reserves its part, not the whole headroom.
#[test]
fn a_replica_whose_fit_cannot_price_reserves_a_part() {
    let ledger = ledger(CARD_MB, no_margin());
    let flat = pre_fit(&ledger, "g/flat", CLIP_BASE_MB, 8);
    let _neighbour = pre_fit(&ledger, "g/tags", TAGS_BASE_MB, 4);
    ledger.install_fit_for_test(
        "g/flat",
        GPU,
        FitSnapshot {
            slope_mb_per_unit: 0.0,
            intercept_mb: 300.0,
            residual_mb: 0.0,
            samples: 20,
            version: 1,
        },
    );
    ledger.record_free_for_test(GPU, HEADROOM_MB);
    assert_eq!(window(&flat).grant().mb, 7666);
}

/// An 8 GiB card with two cold models: the first reserves its seed batch's
/// design cost, more than half the headroom, and the second is cut to the
/// three units the rest covers.
#[test]
fn on_an_8_gib_card_the_second_cold_model_is_cut_to_what_is_left() {
    let ledger = ledger(8192, no_margin());
    let first = pre_fit(&ledger, "g/a", 2000, 8);
    let second = pre_fit(&ledger, "g/b", 2375, 8);
    ledger.record_free_for_test(GPU, 3817);
    let held = window(&first);
    assert_eq!(
        (held.grant().mb, held.grant().unit_budget),
        (SEED_BATCH_MB, 8)
    );
    assert!(!held.grant().squeezed);
    let cut = window(&second);
    assert_eq!((cut.grant().mb, cut.grant().unit_budget), (1257, 3));
    assert!(cut.grant().squeezed);
}

/// A neighbour's unpriced one-unit window is not a reservation: the replica
/// asking beside it keeps its seed batch on less than the batch's price.
#[test]
fn a_neighbours_unpriced_window_does_not_cut_the_batch() {
    let ledger = ledger(8192, no_margin());
    let asking = pre_fit(&ledger, "g/asking", 5000, 4);
    let neighbour = pre_fit(&ledger, "g/neighbour", 2000, 4);
    ledger.record_free_for_test(GPU, 0);
    let unpriced = window(&neighbour);
    assert_eq!((unpriced.grant().mb, unpriced.grant().unit_budget), (0, 1));

    // The other process on the card let go of its 1192 MiB.
    ledger.record_free_for_test(GPU, 1192);
    let token = window(&asking);
    assert_eq!((token.grant().mb, token.grant().unit_budget), (1192, 4));
}

/// A pre-fit replica with seed 8 whose first window ran `batches` of
/// `(units, allocated MiB)` at the default pool margin and kept no pool,
/// beside a neighbour with seed 8, on `headroom` MiB.
fn measured(headroom: u64, batches: &[(u64, u64)]) -> (Arc<VramLedger>, Admission, Admission) {
    let ledger = ledger(headroom + 2000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let measured = ledger
        .register_worker("g/measured", item_cost(8), &handle, None)
        .expect("registers");
    let neighbour = pre_fit(&ledger, "g/neighbour", 1000, 8);
    ledger.record_free_for_test(GPU, headroom);
    let token = window(&measured);
    let batches = batches.iter().map(|(units, allocated)| BatchMeasurement {
        reserved_before_mb: None,
        peak_reserved_mb: None,
        ..measurement(*units, 0, *allocated)
    });
    handle
        .lock()
        .unwrap()
        .record_measurements(batches.collect());
    token.finish(WindowOutcome::Responded { oom: None });
    measured.earn_next_size();
    (ledger, measured, neighbour)
}

/// A replica that has run a batch here is priced from its largest measured
/// batch: each unit more at the design cost while one size is measured, at
/// the rise between the two largest sizes once two are.
#[test]
fn a_measured_replica_is_priced_from_its_largest_batch() {
    // (batches, headroom, the grant and unit budget beside a neighbour's window)
    type Case = (&'static [(u64, u64)], u64, (u64, u64));
    let cases: [Case; 3] = [
        // One size: 1.25 x (1000 + 8 units at the design 256).
        (&[(8, 1000)], 8000, (3810, 16)),
        // Two sizes, 20 MiB a unit: 1.25 x (1000 + 8 x 20).
        (&[(3, 900), (8, 1000)], 4560, (1450, 16)),
        // 1100 MiB left: 880 allocated, which the line reaches at 2 units.
        (&[(3, 900), (8, 1000)], 3660, (1100, 2)),
    ];
    for (batches, headroom, granted) in cases {
        let (_ledger, measured, neighbour) = measured(headroom, batches);
        let held = window(&neighbour);
        assert_eq!(held.grant().mb, (headroom / 2).max(SEED_BATCH_MB));
        let token = window(&measured);
        assert_eq!((token.grant().mb, token.grant().unit_budget), granted);
    }
}

/// No batch is priced under a smaller one that measured more, at the pool
/// margin the replica measured: 4 units allocated 1200 MiB under a pool
/// twice that, then 8 units 1000. A batch of 16, and one of exactly 4, are
/// both 2 x 1200. The fall from 4 to 8 units is no negative cost per unit:
/// 3 units are priced like the 8, 2 x 1000.
#[test]
fn no_batch_is_priced_under_a_measured_batch_of_at_most_its_size() {
    for (window_units, mb) in [(u64::MAX, 2400), (4, 2400), (3, 2000)] {
        let ledger = ledger(7560, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let measured = ledger
            .register_worker("g/measured", item_cost(8), &handle, None)
            .expect("registers");
        let neighbour = pre_fit(&ledger, "g/neighbour", 1000, 8);
        ledger.record_free_for_test(GPU, 5560);
        let token = window(&measured);
        let batches = vec![
            BatchMeasurement {
                peak_allocated_mb: Some(1200),
                ..measurement(4, 0, 2400)
            },
            BatchMeasurement {
                peak_allocated_mb: Some(1000),
                ..measurement(8, 2400, 2400)
            },
        ];
        handle.lock().unwrap().record_measurements(batches);
        // The pool is released after the window.
        push_memory(&handle, 5560, 0);
        token.finish(WindowOutcome::Responded { oom: None });
        measured.earn_next_size();

        let held = window(&neighbour);
        assert_eq!(held.grant().mb, 2780);
        let token = measured
            .request_grant(window_units, None, 1, 0)
            .expect("granted");
        let units = window_units.min(16);
        assert_eq!((token.grant().mb, token.grant().unit_budget), (mb, units));
    }
}

/// A batch smaller than the largest measured one is priced at least at its
/// proportion of that batch: 7 of 8 units that allocated 820 MiB are 1.25 x
/// 717.5, not the 564 that a design unit less would leave.
#[test]
fn a_batch_smaller_than_the_largest_measured_is_priced_at_its_proportion() {
    let (_ledger, measured, neighbour) = measured(4000, &[(8, 820)]);
    let held = window(&neighbour);
    assert_eq!(held.grant().mb, SEED_BATCH_MB);
    let token = measured.request_grant(7, None, 1, 0).expect("granted");
    assert_eq!((token.grant().mb, token.grant().unit_budget), (897, 7));
}

/// With three sizes measured and no slope to price with, each unit more is
/// priced at the rise between the two largest sizes: 12.5 MiB from 4 to 8
/// units, although 2 units allocated more than both.
#[test]
fn the_rise_per_unit_is_taken_between_the_two_largest_sizes() {
    let batches = [(2, 1000), (4, 900), (8, 950)];
    let (_ledger, measured, neighbour) = measured(4560, &batches);
    let _held = window(&neighbour);
    let token = window(&measured);
    // 1.25 x (950 + 8 x 12.5).
    assert_eq!((token.grant().mb, token.grant().unit_budget), (1313, 16));
}

/// A batch that a smaller, later one undercut per unit measured memory that
/// is no longer needed: 8 units allocated 4200 MiB, then 4 units 100. The
/// next 16 units are priced from the 4-unit batch, 1.25 x (100 + 12 x 256).
/// In the other order the 8-unit batch is the later measurement and stands:
/// the 4000 MiB left cover 6 units, six eighths of its 4200 MiB.
#[test]
fn a_batch_a_smaller_later_one_undercut_is_not_a_price() {
    type Batches = &'static [(u64, u64)];
    let cases: [(Batches, (u64, u64)); 2] = [
        (&[(8, 4200), (4, 100)], (3965, 16)),
        (&[(4, 100), (8, 4200)], (4000, 6)),
    ];
    for (batches, granted) in cases {
        let (_ledger, measured, neighbour) = measured(8000, batches);
        let _held = window(&neighbour);
        let token = window(&measured);
        assert_eq!((token.grant().mb, token.grant().unit_budget), granted);
    }
}

/// A batch cut to a size already measured runs the next smaller size not
/// measured yet, until three sizes are: 3 units, then 2, then 1.
#[test]
fn a_batch_cut_to_a_measured_size_runs_a_smaller_one_until_three_are_measured() {
    let ledger = ledger(8192, no_margin());
    let first = pre_fit(&ledger, "g/a", 2000, 8);
    let handle = loaded(Some(2375), Some(0));
    let second = ledger
        .register_worker("g/b", item_cost(8), &handle, None)
        .expect("registers");
    ledger.record_free_for_test(GPU, 3817);
    let _held = window(&first);
    for units in [3, 2, 1] {
        let token = window(&second);
        assert_eq!(token.grant().unit_budget, units);
        assert!(token.grant().squeezed);
        let batch = BatchMeasurement {
            reserved_before_mb: None,
            peak_reserved_mb: None,
            ..measurement(units, 0, units * 256)
        };
        handle.lock().unwrap().record_measurements(vec![batch]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
}

/// Three sizes measured and still no slope to price with: the cut size is
/// left alone, or every window would step one unit further down.
#[test]
fn a_model_with_three_sizes_measured_keeps_its_cut_size() {
    type Batches = &'static [(u64, u64)];
    let cases: [(Batches, u64); 2] = [
        // 2 and 3 are measured: the next smaller size is 1.
        (&[(2, 500), (3, 500)], 1),
        (&[(2, 500), (3, 500), (4, 500)], 3),
    ];
    for (batches, size) in cases {
        let (ledger, _measured, _neighbour) = measured(8000, batches);
        let state = ledger.lock();
        let mut workers = state.workers.values();
        let entry = workers.find(|entry| entry.inference_id == "g/measured");
        let entry = entry.expect("the replica");
        assert_eq!(VramLedger::cut_size_locked(&state, entry, 3), size);
    }
}

/// A replica cut to the one unit it has measured stays at one unit, however
/// much its first batch kept: a second unit would be outside its
/// reservation. It runs more when the neighbour leaves it room.
#[test]
fn a_replica_cut_to_its_one_measured_unit_stays_at_one() {
    const POOL_MB: u64 = 11_238;
    let ledger = ledger(4000 + POOL_MB + SEED_BATCH_MB, no_margin());
    let handle = loaded(Some(2000), Some(0));
    let cut = ledger
        .register_worker("g/cut", item_cost(8), &handle, None)
        .expect("registers");
    let neighbour = pre_fit(&ledger, "g/neighbour", 2000, 8);
    ledger.record_free_for_test(GPU, POOL_MB + SEED_BATCH_MB);
    let token = cut.request_grant(1, None, 1, 0).expect("granted");
    let batch = BatchMeasurement {
        peak_allocated_mb: Some(8990),
        ..measurement(1, 0, POOL_MB)
    };
    handle.lock().unwrap().record_measurements(vec![batch]);
    token.finish(WindowOutcome::Responded { oom: None });

    let held = window(&neighbour);
    assert_eq!(held.grant().mb, SEED_BATCH_MB);
    assert_eq!(ledger.headroom_mb(GPU), 0);
    let token = window(&cut);
    assert_eq!((token.grant().mb, token.grant().unit_budget), (POOL_MB, 1));
    assert!(token.grant().squeezed);
}

/// A replica whose batch size is capped, by memory pressure or after a
/// death, is priced at the capped batch: 2 units at their design cost, not
/// the 8 its ramp admits.
#[test]
fn a_capped_replica_is_priced_at_the_capped_batch() {
    for death in [false, true] {
        let ledger = ledger(5260, no_margin());
        let capped = pre_fit(&ledger, "g/capped", 1000, 8);
        let neighbour = pre_fit(&ledger, "g/neighbour", 1000, 8);
        ledger.record_free_for_test(GPU, 3260);
        {
            let mut state = ledger.lock();
            let key = ("g/capped".to_owned(), GPU.to_owned());
            let cal = state.calibration.entry(key).or_default();
            if death {
                cal.death_cap_units = Some(2);
            } else {
                cal.pressure_cap = Some(PressureCap {
                    units: 2,
                    regrow_to: 2,
                    halved_at: None,
                });
            }
        }
        let held = window(&neighbour);
        assert_eq!(held.grant().mb, SEED_BATCH_MB);
        assert_eq!(ledger.headroom_mb(GPU), 700);

        let token = window(&capped);
        assert_eq!((token.grant().mb, token.grant().unit_budget), (640, 2));
        assert!(!token.grant().squeezed);
    }
}

/// A grant one MiB short of a batch's price does not cover it: 959 MiB are
/// 2 units of 320, not 3.
#[test]
fn a_grant_one_mib_short_of_a_batchs_price_does_not_cover_it() {
    let ledger = ledger(7519, no_margin());
    let first = pre_fit(&ledger, "g/a", 2000, 8);
    let second = pre_fit(&ledger, "g/b", 2000, 8);
    ledger.record_free_for_test(GPU, 3519);
    let _held = window(&first);
    let cut = window(&second);
    assert_eq!((cut.grant().mb, cut.grant().unit_budget), (959, 2));
}

/// On the CPU device memory a first batch kept stays resident and is
/// charged as the replica's own, so the next batch's price is raised from
/// the headroom only by what it needs beyond that: 1.25 x (8990 + 256) less
/// the 8900 MiB held.
#[test]
fn memory_a_cpu_replica_keeps_is_not_reserved_again() {
    const RAM_MB: u64 = 25_200;
    let ledger = VramLedger::new(
        &crate::inferio::gpu::GpuInventory::known_cpu(RAM_MB),
        VramBudget::default().into(),
        None,
    );
    let handle = loaded_cpu(Some(RAM_MB));
    let keeping = ledger
        .register_worker("g/keeping", item_cost(8), &handle, None)
        .expect("registers");
    let neighbour = ledger
        .register_worker("g/neighbour", item_cost(8), &loaded_cpu(Some(RAM_MB)), None)
        .expect("registers");
    ledger.record_free_for_test(cpu::DEVICE_KEY, RAM_MB - 2000);
    let token = keeping.request_grant(1, None, 1, 0).expect("granted");
    let batch = BatchMeasurement {
        reserved_before_mb: None,
        peak_reserved_mb: None,
        rss_after_mb: Some(8900),
        ..measurement(1, 0, 8990)
    };
    handle.lock().unwrap().record_measurements(vec![batch]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.headroom_mb(cpu::DEVICE_KEY), 8000);

    let held = window(&neighbour);
    assert_eq!(held.grant().mb, 4000);
    let token = window(&keeping);
    assert_eq!((token.grant().mb, token.grant().unit_budget), (2658, 2));
    assert!(!token.grant().squeezed);
}

/// A price that is not a whole MiB is rounded up, so the reservation covers
/// the batch it was raised for: 100 units of a 192-unit seed are 1333.3 MiB.
#[test]
fn a_fractional_price_is_rounded_up_and_does_not_cut_the_batch() {
    let ledger = ledger(6560, no_margin());
    let asking = pre_fit(&ledger, "g/asking", 1000, 192);
    let neighbour = pre_fit(&ledger, "g/neighbour", 1000, 8);
    ledger.record_free_for_test(GPU, 4560);
    let held = window(&neighbour);
    assert_eq!(held.grant().mb, SEED_BATCH_MB);

    let token = asking.request_grant(100, None, 1, 0).expect("granted");
    assert_eq!((token.grant().mb, token.grant().unit_budget), (1334, 100));
    assert!(!token.grant().squeezed);
}

/// A batch that measured no growth is no price: the design cost stays.
#[test]
fn a_batch_that_measured_no_growth_leaves_the_design_price() {
    let ledger = ledger(8192, no_margin());
    let first = pre_fit(&ledger, "g/a", 2000, 8);
    let handle = loaded(Some(2375), Some(0));
    let second = ledger
        .register_worker("g/b", item_cost(8), &handle, None)
        .expect("registers");
    ledger.record_free_for_test(GPU, 3817);
    let token = window(&second);
    let flat = vec![measurement(8, 0, 0)];
    handle.lock().unwrap().record_measurements(flat);
    token.finish(WindowOutcome::Responded { oom: None });

    let _held = window(&first);
    let cut = window(&second);
    assert_eq!((cut.grant().mb, cut.grant().unit_budget), (1257, 3));
}

/// With less headroom than its seed batch is designed to cost and nobody in
/// a window, a cold model still takes all of it and runs the seed batch, as
/// it does alone; the neighbour's window is then unpriced, one unit.
#[test]
fn a_cold_model_on_a_nearly_full_card_runs_its_seed_batch_first() {
    let ledger = ledger(8192, no_margin());
    let large = loaded(Some(5000), Some(0));
    let asking = ledger
        .register_worker("g/large", item_cost(4), &large, None)
        .expect("registers");
    let neighbour = pre_fit(&ledger, "g/neighbour", 2000, 4);

    // 392 MiB of headroom and 800 MiB of the requester's own pool.
    push_memory(&large, 392, 800);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 392);
    let token = window(&asking);
    assert_eq!(token.grant().mb, 392 + 800);
    assert_eq!(token.grant().unit_budget, 4);

    let other = window(&neighbour);
    assert_eq!((other.grant().mb, other.grant().unit_budget), (0, 1));
}

/// A replica whose pool already covers its batch reserves the contention
/// floor on top, above half the headroom; the neighbour is cut to the one
/// unit the rest covers.
#[test]
fn a_replica_whose_pool_covers_its_batch_still_reserves_the_floor() {
    let ledger = ledger(10_392, no_margin());
    let large = loaded(Some(5000), Some(0));
    let asking = ledger
        .register_worker("g/large", item_cost(4), &large, None)
        .expect("registers");
    let neighbour = pre_fit(&ledger, "g/neighbour", 2000, 4);

    // 392 MiB of headroom and 3000 MiB of the requester's own pool.
    push_memory(&large, 392, 3000);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 392);
    let token = window(&asking);
    assert_eq!(token.grant().mb, SEED_BATCH_FLOOR_MB + 3000);
    assert_eq!(token.grant().unit_budget, 4);
    assert!(!token.grant().squeezed);

    let other = window(&neighbour);
    assert_eq!((other.grant().mb, other.grant().unit_budget), (136, 1));
    assert!(other.grant().squeezed);
}

/// The CPU device: two pre-fit CPU replicas share the RAM headroom the same
/// way.
#[test]
fn two_pre_fit_cpu_replicas_each_reserve_a_part_of_the_ram() {
    let ledger = cpu_ledger(no_margin());
    let replicas: Vec<Admission> = ["g/a", "g/b"]
        .iter()
        .map(|model| {
            ledger
                .register_worker(model, item_cost(4), &loaded_cpu(Some(CPU_RAM_MB)), None)
                .expect("registers")
        })
        .collect();
    // Nothing else holds RAM: both bases are ours.
    ledger.record_free_for_test(cpu::DEVICE_KEY, CPU_RAM_MB - 2000);
    let headroom = ledger.headroom_mb(cpu::DEVICE_KEY);

    let first = window(&replicas[0]);
    assert_eq!(first.grant().mb, headroom / 2);
    let second = window(&replicas[1]);
    assert_eq!(second.grant().mb, (headroom - headroom / 2) / 2);
    assert_eq!(second.grant().unit_budget, 4);
}

/// The CPU device's headroom is what the RAM reserve leaves (a tenth of a
/// 32 GB machine), and a pre-fit share is a part of that.
#[test]
fn a_pre_fit_cpu_share_is_a_part_of_the_headroom_under_the_ram_reserve() {
    const RAM_MB: u64 = 32_000;
    let budget = VramBudget {
        cap_fraction: Some(1.0),
        ..no_margin()
    };
    let ledger = VramLedger::new(
        &crate::inferio::gpu::GpuInventory::known_cpu(RAM_MB),
        budget.into(),
        None,
    );
    let replicas: Vec<Admission> = ["g/a", "g/b"]
        .iter()
        .map(|model| {
            ledger
                .register_worker(model, item_cost(4), &loaded_cpu(Some(RAM_MB)), None)
                .expect("registers")
        })
        .collect();
    ledger.record_free_for_test(cpu::DEVICE_KEY, RAM_MB - 2000);
    let headroom = ledger.headroom_mb(cpu::DEVICE_KEY);
    assert_eq!(headroom, RAM_MB - 3200 - 2000);
    assert_eq!(window(&replicas[0]).grant().mb, headroom / 2);
}

/// Alone on its device, a pre-fit replica whose batches measured their price
/// is cut to the batch its room covers at that price; while the room covers
/// the batch nothing changes, and one that measured nothing gets its seed.
#[test]
fn a_measured_pre_fit_batch_is_cut_to_the_room_when_alone() {
    const PER_UNIT_MB: u64 = 82;
    for (room, expected) in [(1_500, 18), (8_000, 24)] {
        let ledger = ledger(554 + room, no_margin());
        let handle = loaded(Some(554), Some(0));
        let alone = ledger
            .register_worker("g/alone", item_cost(64), &handle, None)
            .expect("registers");
        ledger.record_free_for_test(GPU, room);
        let unmeasured = window(&alone);
        assert_eq!(
            (unmeasured.grant().unit_budget, unmeasured.grant().squeezed),
            (64, false),
            "nothing measured: the seed"
        );
        drop(unmeasured);
        // Two queue-sized windows measure 82 MiB per unit; the ratchet then
        // admits twice the larger.
        let mut pool = 0;
        for units in [8, 12] {
            let token = alone.request_grant(units, None, 1, 0).expect("granted");
            let allocated = PER_UNIT_MB * units;
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![BatchMeasurement {
                    reserved_before_mb: Some(pool),
                    reserved_after_mb: Some(allocated),
                    allocated_before_mb: Some(0),
                    peak_allocated_mb: Some(allocated),
                    ..measurement(units, 0, allocated)
                }]);
            token.finish(WindowOutcome::Responded { oom: None });
            pool = allocated;
            ledger.record_free_for_test(GPU, room - pool);
        }
        let full = window(&alone);
        let grant = full.grant();
        assert_eq!(grant.unit_budget, expected, "room {room}");
        assert_eq!(grant.squeezed, expected < 24);
        assert!(PER_UNIT_MB * grant.unit_budget <= room);
    }
}
