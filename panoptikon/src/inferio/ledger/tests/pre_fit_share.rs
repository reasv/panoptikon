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

/// A replica that has run a batch here is priced at what that batch
/// measured, times the pool margin: a larger batch by scaling the largest
/// one measured, and no batch under a smaller one that was measured.
#[test]
fn a_measured_replica_is_priced_at_its_largest_batch_and_never_under_a_measurement() {
    // 3 units allocated 900 MiB and 8 units 1000: a large fixed part.
    // (headroom, window, the grant and unit budget beside a neighbour's window)
    let cases = [
        // 16 units: 2 x 1000 x 1.25.
        (6560, u64::MAX, (2500, 16)),
        // 1100 MiB left: by that scale 7 units, but 3 measured 1125, so 2.
        (3660, u64::MAX, (1100, 2)),
        // A 4-unit window: not 4/8 of 1250, but the 1125 that 3 measured.
        (4560, 4, (1125, 4)),
    ];
    for (headroom, window_units, granted) in cases {
        let ledger = ledger(headroom + 2000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let measured = ledger
            .register_worker("g/measured", item_cost(8), &handle, None)
            .expect("registers");
        let neighbour = pre_fit(&ledger, "g/neighbour", 1000, 8);
        ledger.record_free_for_test(GPU, headroom);
        let batch = |units: u64, allocated: u64| BatchMeasurement {
            reserved_before_mb: None,
            peak_reserved_mb: None,
            ..measurement(units, 0, allocated)
        };
        let token = window(&measured);
        let batches = vec![batch(3, 900), batch(8, 1000)];
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });

        let held = window(&neighbour);
        assert_eq!(held.grant().mb, (headroom / 2).max(SEED_BATCH_MB));
        let token = measured
            .request_grant(window_units, None, 1, 0)
            .expect("granted");
        assert_eq!((token.grant().mb, token.grant().unit_budget), granted);
    }
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
