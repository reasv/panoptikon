//! Pre-fit reservations: at most an equal part of the headroom among the
//! replicas on the device, so the first to ask does not hold the others at
//! one unit.
use super::*;

/// A 16 GiB card with 15 333 MiB of headroom over two pre-fit models.
const CARD_MB: u64 = 16_368;
const CLIP_BASE_MB: u64 = 435;
const TAGS_BASE_MB: u64 = 600;
const HEADROOM_MB: u64 = CARD_MB - CLIP_BASE_MB - TAGS_BASE_MB;

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
/// sees, and headroom is left over.
#[test]
fn three_pre_fit_replicas_each_reserve_a_third_of_what_they_see() {
    let ledger = ledger(18_000, no_margin());
    let replicas: Vec<Admission> = (0..3)
        .map(|index| pre_fit(&ledger, &format!("g/m{index}"), 1000, 4))
        .collect();
    ledger.record_free_for_test(GPU, 15_000);
    let tokens: Vec<GrantToken> = replicas.iter().map(window).collect();
    let reserved: Vec<u64> = tokens.iter().map(|token| token.grant().mb).collect();
    assert_eq!(reserved, vec![5000, 3333, 2222]);
    assert!(
        tokens
            .iter()
            .all(|token| token.grant().unit_budget == 4 && !token.grant().squeezed)
    );
    assert_eq!(ledger.headroom_mb(GPU), 15_000 - 10_555);
}

/// A pre-fit replica joining two fitted ones that are in a window reserves a
/// third of the headroom, so a fitted one can still grow its next window.
/// Only the pre-fit share is cut: the fitted one takes more than a third.
#[test]
fn a_pre_fit_replica_joining_busy_fitted_ones_leaves_them_room_to_grow() {
    let ledger = ledger(9600, no_margin());
    // 100 MiB per unit, measured up to 24 units, which left a 2400 MiB pool.
    let fitted: Vec<(TelemetryHandle, Admission)> = (0..2)
        .map(|index| {
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker(&format!("g/fitted{index}"), item_cost(4), &handle, None)
                .expect("registers");
            let series = (1..=6u64).map(|k| measurement(k * 4, 0, 100 * k * 4));
            handle.lock().unwrap().record_measurements(series.collect());
            clean_window(&admission);
            (handle, admission)
        })
        .collect();
    let joining = pre_fit(&ledger, "g/joining", 1000, 4);
    // 3 bases and 2 pools are ours: 9600 - 3000 - 4800 is free.
    ledger.record_free_for_test(GPU, 1800);

    let busy: Vec<GrantToken> = fitted
        .iter()
        .map(|(_, admission)| admission.request_grant(24, None, 1, 0).expect("granted"))
        .collect();
    assert!(busy.iter().all(|token| token.grant().mb == 2400));
    assert_eq!(ledger.headroom_mb(GPU), 1800, "spent inside their pools");

    let token = window(&joining);
    assert_eq!(token.grant().mb, 600, "1800 / 3");
    assert_eq!(token.grant().unit_budget, 4);

    // The first fitted replica's next window is 8 units larger: its pool
    // and 800 of the 1200 left, more than a third of it.
    let mut busy = busy.into_iter();
    drop(busy.next());
    let grown = window(&fitted[0].1);
    assert_eq!(grown.grant().unit_budget, 32);
    assert_eq!(grown.grant().mb, 3200);
    assert!(!grown.grant().squeezed);
}

/// An 8 GiB card with little headroom: a part is still a priced grant for
/// the seed batch, the requester's own pool is added after the division,
/// and a neighbour is not left with nothing.
#[test]
fn on_a_small_card_a_part_is_still_a_priced_seed_batch() {
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
    assert_eq!(token.grant().mb, 256 + 800, "the floor, above 392 / 2");
    assert_eq!(token.grant().unit_budget, 4);
    assert!(!token.grant().squeezed);

    let other = window(&neighbour);
    assert_eq!(other.grant().mb, 136, "what is left, under its floor");
    assert_eq!(other.grant().unit_budget, 4, "priced, so not one unit");
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
