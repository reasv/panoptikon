//! Load reservations, and the refusal of loads that cannot fit.
use super::*;

/// A load reservation is charged from load-start and released on drop, at
/// the base this run measured for the same (model, GPU) once there is one.
#[tokio::test]
async fn load_reservations_charge_and_release() {
    let ledger = ledger(10_000, no_margin());
    assert_eq!(ledger.headroom_mb(GPU), 10_000);
    let reservation = ledger
        .reserve_load_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("known GPU");
    assert_eq!(
        ledger.headroom_mb(GPU),
        10_000 - CONSERVATIVE_BASE_MB,
        "an unmeasured first load reserves the conservative constant"
    );
    assert_eq!(
        ledger.health()[0].load_reservations_mb,
        CONSERVATIVE_BASE_MB
    );
    drop(reservation);
    assert_eq!(ledger.headroom_mb(GPU), 10_000, "released on drop");

    // A measured load teaches the ledger the real base for next time.
    let handle = loaded(Some(1234), Some(0));
    let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
    let reservation = ledger
        .reserve_load_for_test("g/a", item_cost(4), GPU, None)
        .await
        .unwrap();
    assert_eq!(
        ledger.headroom_mb(GPU),
        10_000 - 1234 - 1234,
        "remembered base beats the conservative constant"
    );
    drop(reservation);
    // An unknown GPU has nothing to charge against.
    assert!(
        ledger
            .reserve_load_for_test("g/a", item_cost(4), "GPU-nope", None)
            .await
            .is_none()
    );
}

/// A model whose **known** base is larger than everything the card can
/// lend is refused before a worker is spawned, with both numbers in the
/// refusal: admitting it buys an out-of-memory per item.
#[tokio::test]
async fn a_base_larger_than_the_cards_room_refuses_the_load() {
    // The shipped row for a model of this size, against a 32 GB card with
    // a desktop holding 6 GB of it.
    let profiles = Arc::new(FakeProfiles {
        base: Some(31_752),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(32_607, no_margin(), &profiles);
    let handle = loaded(Some(1_000), Some(0));
    let _resident = ledger
        .register_worker("g/small", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 32_607 - 6_000 - 1_000, 0);
    ledger.ingest_all_for_test();
    let Err(refusal) = ledger
        .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
        .await
    else {
        panic!("a base of 31 752 MiB does not fit 26 607 MiB of room");
    };
    assert_eq!(refusal.needs_mb, 31_752);
    assert_eq!(
        refusal.room_mb,
        32_607 - 6_000,
        "the card's limit, before any of our own residents are charged"
    );
    assert!(
        refusal.to_string().contains("clip/qwen3-vl-embedding-8b"),
        "the model is named: {refusal}"
    );
    assert_eq!(
        ledger.health()[0].load_reservations_mb,
        0,
        "nothing is charged for a load that will not be attempted"
    );
}

/// The same refusal on a base this run **measured**: the next load of that
/// (model, GPU) is priced against it.
#[tokio::test]
async fn a_measured_base_over_the_room_refuses_the_next_load() {
    let ledger = ledger(32_607, no_margin());
    let big = loaded(Some(31_595), Some(31_202));
    let admission = ledger
        .register_worker("clip/qwen3", item_cost(4), &big, None)
        .expect("registers");
    // The storm ended and the replica went away; the card is measured
    // again with only the desktop's 6 GB on it.
    drop(admission);
    let handle = loaded(Some(1_000), Some(0));
    let _resident = ledger
        .register_worker("g/small", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 32_607 - 6_000 - 1_000, 0);
    ledger.ingest_all_for_test();
    let Err(refusal) = ledger
        .reserve_load("clip/qwen3", item_cost(4), GPU, None)
        .await
    else {
        panic!("the measured base does not fit the card's room");
    };
    assert_eq!(refusal.needs_mb, 31_595, "the measured base, not a profile");
    assert_eq!(refusal.room_mb, 32_607 - 6_000);
}

/// The reserve is a batch-time margin, not a veto on loading: a 670 MiB model
/// is loaded on a card with 981 MiB free, though a hog there withholds the
/// whole capped default reserve.
#[tokio::test]
async fn the_reserve_does_not_refuse_a_model_the_card_has_room_for() {
    let profiles = Arc::new(FakeProfiles {
        base: Some(670),
        ..FakeProfiles::default()
    });
    // The default budget: an unset margin, hence the capped default
    // reserve, which is larger than everything this card has left.
    let ledger = ledger_with(24_576, VramBudget::default(), &profiles);
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 24_576,
        free_mb: 981,
    }]));
    let reservation = ledger
        .reserve_load("tags/wd-vit-tagger-v3", item_cost(4), GPU, None)
        .await
        .expect("670 MiB fits the 981 MiB the card has free");
    assert!(reservation.is_some(), "a known GPU charges the load");
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.reserve_mb, DEFAULT_RESERVE_CAP_MB);
    assert_eq!(gpu.limit_mb, 0, "the batch budget is zero, and may be");
    assert_eq!(
        gpu.load_reservations_mb, 0,
        "the reservation is clamped to that headroom, as before"
    );
}

/// What the card does not have free is still refused, reserve or no reserve.
#[tokio::test]
async fn a_base_over_what_the_card_has_free_is_refused() {
    let profiles = Arc::new(FakeProfiles {
        base: Some(670),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(24_576, VramBudget::default(), &profiles);
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 24_576,
        free_mb: 500,
    }]));
    let Err(refusal) = ledger
        .reserve_load("tags/wd-vit-tagger-v3", item_cost(4), GPU, None)
        .await
    else {
        panic!("670 MiB does not fit 500 MiB of free VRAM");
    };
    assert_eq!(refusal.needs_mb, 670);
    assert_eq!(
        refusal.room_mb, 500,
        "what the card has, before the reserve"
    );
}

/// Dropping the reserve from the comparand does not rescue a model that is
/// genuinely too big: 31 752 MiB on a 32 607 MiB card with 1 316 MiB of
/// desktop on it is over the room either way.
#[tokio::test]
async fn the_5090s_oversized_model_is_refused_without_the_reserve_too() {
    let profiles = Arc::new(FakeProfiles {
        base: Some(31_752),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(32_607, VramBudget::default(), &profiles);
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 32_607,
        free_mb: 32_607 - 1_316,
    }]));
    let Err(refusal) = ledger
        .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
        .await
    else {
        panic!("31 752 MiB does not fit a card with a desktop on it");
    };
    assert_eq!(refusal.needs_mb, 31_752);
    assert_eq!(refusal.room_mb, 31_291);
}

/// A base the ledger only *guesses* refuses nothing: refusing on the
/// conservative constant would stop a first load on every small card.
#[tokio::test]
async fn an_unmeasured_load_is_never_refused_for_size() {
    let ledger = ledger(CONSERVATIVE_BASE_MB / 2, no_margin());
    let reservation = ledger
        .reserve_load("g/a", item_cost(4), GPU, None)
        .await
        .expect("not refused on a guess");
    assert!(
        reservation.is_some(),
        "it is still charged, clamped to the headroom"
    );
}

/// The refusal judges the card's **whole limit**, never the room left
/// after our own residents: a base that fits once the ledger evicts its
/// idle models is the evict-before-load *signal*, not a refusal.
#[tokio::test]
async fn our_own_residents_are_not_a_reason_to_refuse_a_load() {
    let profiles = Arc::new(FakeProfiles {
        base: Some(25_000),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(32_607, no_margin(), &profiles);
    // 20 GB of *our* idle model on an otherwise empty card.
    let handle = loaded(Some(20_000), Some(0));
    let _resident = ledger
        .register_worker("g/resident", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 32_607 - 20_000, 0);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].external_mb, 0, "nothing external");
    assert_eq!(ledger.health()[0].headroom_mb, 12_607, "ours, not theirs");
    let (_reservation, exceeds_headroom) = ledger
        .reserve_load_signalling("g/big", item_cost(4), GPU, None)
        .await
        .expect("no refusal: unloading the resident makes room")
        .expect("a known GPU charges the load");
    assert!(
        exceeds_headroom,
        "it is the evict-before-load signal instead"
    );
}

/// A profile row may not **veto** what this card measured itself: a shipped
/// row from a bigger board must not refuse a model that loaded and ran here.
/// The reservation still takes the larger of the two.
#[tokio::test]
async fn this_runs_measurement_outranks_a_profile_row_for_the_refusal() {
    let profiles = Arc::new(FakeProfiles {
        base: Some(31_752),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(32_607, no_margin(), &profiles);
    // The model loaded here at 17 000 MiB and ran; then it was unloaded.
    let handle = loaded(Some(17_000), Some(0));
    let admission = ledger
        .register_worker("clip/qwen3-vl-embedding-8b", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 32_607 - 6_000 - 17_000, 0);
    ledger.ingest_all_for_test();
    drop(admission);
    let handle = loaded(Some(1_000), Some(0));
    let _other = ledger
        .register_worker("g/small", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 32_607 - 6_000 - 1_000, 0);
    ledger.ingest_all_for_test();
    let reservation = ledger
        .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
        .await
        .expect("17 000 MiB fitted this card once and still does");
    assert!(reservation.is_some());
    assert_eq!(
        ledger.health()[0].load_reservations_mb,
        32_607 - 6_000 - 1_000,
        "the profile's bigger number is still held, clamped to headroom"
    );
}

/// On a unified-memory device `external` is every other process's RAM, so a
/// model this Mac measured at 40 GB is **attempted** under memory pressure;
/// only a model larger than the machine is refused.
#[tokio::test]
async fn a_mac_under_ram_pressure_still_attempts_a_model_it_ran_before() {
    async fn after_the_ram_went(base_mb: u64) -> Result<Option<LoadReservation>, OversizedLoad> {
        let ledger = mps_ledger();
        let handle = loaded_mps(Some(MAC_RAM_MB / 10 * 9));
        handle
            .lock()
            .unwrap()
            .load
            .as_mut()
            .expect("a load report")
            .value
            .base_mb = Some(base_mb);
        let admission = ledger
            .register_worker("clip/big", item_cost(4), &handle, None)
            .expect("registers");
        drop(admission);
        // Something else took the machine's RAM while the model was unloaded.
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: MPS_GPU.to_owned(),
            total_mb: MAC_RAM_MB,
            free_mb: 30_000,
        }]));
        ledger
            .reserve_load("clip/big", item_cost(4), MPS_GPU, None)
            .await
    }
    assert!(
        after_the_ram_went(40_000).await.is_ok(),
        "30 GB of free RAM is pressure, not a verdict on the model"
    );
    let Err(refusal) = after_the_ram_went(MAC_RAM_MB + 10_000).await else {
        panic!("a model larger than the machine is refused whatever is free");
    };
    assert_eq!(refusal.needs_mb, MAC_RAM_MB + 10_000);
    assert_eq!(
        refusal.room_mb,
        MAC_RAM_MB / 10 * 9,
        "the device's capacity, with no volatile external in it"
    );
}

/// A condemned replica's next load of that pair is judged against the
/// **working set** (the base plus more room than the failing window had),
/// where the base alone fits and would be reloaded at once.
#[tokio::test]
async fn a_condemned_replicas_working_set_refuses_the_reload() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(9_900), Some(0));
    let admission = ledger
        .register_worker("g/big", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 0, 0);
    ledger.ingest_all_for_test();
    let mut verdict = None;
    for _ in 0..OOM_WINDOWS_AT_FLOOR {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        verdict = token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
    }
    let verdict = verdict.expect("condemned");
    assert_eq!(
        verdict.base_mb, verdict.room_mb,
        "the base is the whole card"
    );
    assert_eq!(
        verdict.needs_mb, 9_901,
        "just over the reserve-less room, the comparand the reload is \
         judged on"
    );
    // The dispatcher kills the worker and the manager drops the model.
    drop(admission);
    push_memory(&handle, 10_000, 0);
    ledger.ingest_all_for_test();
    let Err(refusal) = ledger.reserve_load("g/big", item_cost(4), GPU, None).await else {
        panic!("the base fits the emptied card; the working set does not");
    };
    assert_eq!(refusal.needs_mb, 9_901);
    assert_eq!(
        refusal.room_mb, 9_900,
        "the base alone is not *over* this room, which is why the base \
         alone reloaded the same worker"
    );
}

/// On a memory-blind grant `base + room + 1` pins at the base, which would
/// re-admit the condemned model forever. The stored figure is floored just
/// over the reserve-less room instead, so a card that later frees more
/// still tries.
#[tokio::test]
async fn a_memory_blind_condemnation_refuses_the_reload_on_an_unchanged_card() {
    let profiles = Arc::new(FakeProfiles {
        base: Some(670),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(24_576, VramBudget::default(), &profiles);
    let handle = loaded(Some(670), Some(0));
    let admission = ledger
        .register_worker("tags/wd-vit-tagger-v3", item_cost(4), &handle, None)
        .expect("registers");
    // 981 MiB of reserve-less room, 670 of it this model's: the capped
    // reserve withholds more than the 311 left, so every window is blind.
    push_memory(&handle, 311, 0);
    ledger.ingest_all_for_test();
    let mut verdict = None;
    for _ in 0..OOM_WINDOWS_AT_FLOOR {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().mb, 0, "memory-blind: no room priced");
        verdict = token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
    }
    let verdict = verdict.expect("condemned");
    assert_eq!(
        verdict.needs_mb, 982,
        "the reserve-less room and one more, not the base plus nothing"
    );
    drop(admission);
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 24_576,
        free_mb: 981,
    }]));
    let Err(refusal) = ledger
        .reserve_load("tags/wd-vit-tagger-v3", item_cost(4), GPU, None)
        .await
    else {
        panic!("the unchanged card re-admits the condemned model");
    };
    assert_eq!((refusal.needs_mb, refusal.room_mb), (982, 981));
    // A neighbour loads and its readings show the card with 1 500 MiB.
    let neighbour = loaded(Some(0), Some(0));
    let _neighbour = ledger
        .register_worker("g/neighbour", item_cost(4), &neighbour, None)
        .expect("registers");
    push_memory(&neighbour, 1_500, 0);
    ledger.ingest_all_for_test();
    assert!(
        ledger
            .reserve_load("tags/wd-vit-tagger-v3", item_cost(4), GPU, None)
            .await
            .is_ok(),
        "a card that freed more than the stored figure tries again"
    );
}

/// A card the model truly cannot run one item on converges on a refusal,
/// and each condemnation raises the remembered figure by the room the
/// failing window had, bounded by the price of one item
/// ([`PRE_FIT_ONE_UNIT_BASE_DIVISOR`]) rather than the whole base.
#[tokio::test]
async fn the_remembered_working_set_climbs_until_it_refuses() {
    let ledger = ledger(32_607, no_margin());
    let bound = 31_150 + 31_150 / PRE_FIT_ONE_UNIT_BASE_DIVISOR + 1;
    let handle = loaded(Some(31_150), Some(0));
    let admission = ledger
        .register_worker("clip/qwen3-vl-embedding-8b", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 456, 0);
    ledger.ingest_all_for_test();
    let mut verdict = None;
    for _ in 0..OOM_WINDOWS_AT_FLOOR {
        let token = admission.request_grant(1, None, 1, 0).expect("granted");
        assert_eq!(token.grant().unit_budget, 1, "one item in hand");
        verdict = token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
    }
    let first = verdict.expect("condemned: 305 MiB does not run an item of it");
    assert_eq!(first.needs_mb, 31_607);
    assert!(first.needs_mb <= bound, "bounded: {}", first.needs_mb);
    drop(admission);
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 32_607,
        free_mb: 32_607,
    }]));
    // Cycle two: the card has more room than the figure that condemned it,
    // so the reload is admitted. An unchanged card would refuse it.
    let reservation = ledger
        .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
        .await
        .expect("not refused: the working set is under the emptied card")
        .expect("a known GPU charges the load");
    drop(reservation);
    let handle = loaded(Some(31_150), Some(0));
    let admission = ledger
        .register_worker("clip/qwen3-vl-embedding-8b", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 3_000, 0);
    ledger.ingest_all_for_test();
    let mut verdict = None;
    for _ in 0..OOM_WINDOWS_AT_FLOOR {
        let token = admission.request_grant(1, None, 1, 0).expect("granted");
        verdict = token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
    }
    let second = verdict.expect("condemned again, on a roomier card");
    assert!(
        second.needs_mb > first.needs_mb && second.needs_mb <= bound,
        "the climb is one item's price per cycle, not one base: {} then {}",
        first.needs_mb,
        second.needs_mb
    );
    drop(admission);
    push_memory(&handle, 32_607, 0);
    ledger.ingest_all_for_test();
    let Err(refusal) = ledger
        .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
        .await
    else {
        panic!("the climbed working set finally refuses the reload");
    };
    assert_eq!(refusal.needs_mb, second.needs_mb);
    assert_eq!(refusal.room_mb, 32_607, "the whole empty card");
}

/// Pre-fit, one item is priced at a lower bound, not at the whole base: a
/// replica with 30 GB of room is not at the floor however large the weights.
#[test]
fn a_pre_fit_one_item_oom_with_room_under_the_base_condemns_nothing() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(60_000), Some(0));
    let admission = ledger
        .register_worker("g/big", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 30_000, 0);
    ledger.ingest_all_for_test();
    for _ in 0..(4 * OOM_WINDOWS_AT_FLOOR) {
        let token = admission.request_grant(1, None, 1, 0).expect("granted");
        assert_eq!(token.grant().unit_budget, 1, "one item in hand");
        assert!(
            token.grant().mb > 20_000,
            "tens of GB of room, not a squeeze to nothing"
        );
        assert!(
            token
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                })
                .is_none(),
            "an out-of-memory with room in hand is the backstop's business"
        );
    }
}

/// A clean window clears a condemnation; a load coming up does not, since
/// the condemnation already granted that the weights fit.
#[tokio::test]
async fn a_clean_window_clears_the_remembered_working_set() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(9_900), Some(0));
    let admission = ledger
        .register_worker("g/big", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 0, 0);
    ledger.ingest_all_for_test();
    let mut verdict = None;
    for _ in 0..OOM_WINDOWS_AT_FLOOR {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        verdict = token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
    }
    verdict.expect("condemned");
    assert!(ledger.was_condemned("g/big", GPU));
    assert!(
        !ledger.was_condemned("g/big", "GPU-elsewhere"),
        "keyed per GPU"
    );
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert!(
        token
            .finish(WindowOutcome::Responded { oom: None })
            .is_none()
    );
    assert!(!ledger.was_condemned("g/big", GPU), "a window ran here");
    drop(admission);
    push_memory(&handle, 10_000, 0);
    ledger.ingest_all_for_test();
    assert!(
        ledger
            .reserve_load("g/big", item_cost(4), GPU, None)
            .await
            .is_ok(),
        "and the reload is no longer refused"
    );
}

/// WDDM's sysmem fallback answers a window that does not fit with a
/// **throughput collapse**, never an out-of-memory. The floor rule reads
/// memory failures only, so the collapse condemns nothing.
#[test]
fn a_collapse_at_the_one_item_floor_condemns_nothing() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(9_900), Some(0));
    let admission = ledger
        .register_worker("g/big", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 0, 0);
    ledger.ingest_all_for_test();
    for _ in 0..(4 * OOM_WINDOWS_AT_FLOOR) {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().mb, 0);
        assert_eq!(token.grant().unit_budget, 1);
        let mut collapsed = measurement(1, 0, 0);
        collapsed.throughput_collapse = true;
        handle.lock().unwrap().record_measurements(vec![collapsed]);
        assert!(
            token
                .finish(WindowOutcome::Responded { oom: None })
                .is_none(),
            "a collapse at the floor is not an out-of-memory, so it never \
             condemns the replica"
        );
    }
}

/// The calibration store supplies the expected base of a first-ever load,
/// which has no dtype or torch build yet, so the store answers with its
/// most conservative figure.
#[tokio::test]
async fn profile_lookup_supplies_the_expected_base() {
    let profiles = Arc::new(FakeProfiles {
        base: Some(777),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(10_000, no_margin(), &profiles);
    let _reservation = ledger
        .reserve_load_for_test("g/a", item_cost(4), GPU, None)
        .await
        .unwrap();
    assert_eq!(ledger.headroom_mb(GPU), 10_000 - 777);
    let queries = profiles.queries.lock().unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].0, "g/a");
    assert_eq!(queries[0].1, 1, "the model's epoch is part of the key");
    assert_eq!(
        queries[0].2, ARCH,
        "the GPU's architecture, not its SKU name and not its UUID"
    );
    assert_eq!(
        queries[0].3, None,
        "no torch build before the load response"
    );
    assert_eq!(
        queries[0].4, None,
        "and no negotiated dtype on a first load"
    );
}

/// Two sources describe the same quantity — this run's measured base and the stored
/// profile's — so the reservation takes the larger.
#[tokio::test]
async fn the_load_reservation_takes_the_more_conservative_base() {
    let profiles = Arc::new(FakeProfiles {
        base: Some(5000),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(20_000, no_margin(), &profiles);
    let handle = loaded(Some(1234), Some(0));
    let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
    let reservation = ledger
        .reserve_load_for_test("g/a", item_cost(4), GPU, None)
        .await
        .unwrap();
    assert_eq!(
        ledger.health()[0].load_reservations_mb,
        5000,
        "the profile's larger base wins over this run's measurement"
    );
    drop(reservation);

    let profiles = Arc::new(FakeProfiles {
        base: Some(100),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(20_000, no_margin(), &profiles);
    let handle = loaded(Some(1234), Some(0));
    let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
    let _reservation = ledger
        .reserve_load_for_test("g/a", item_cost(4), GPU, None)
        .await
        .unwrap();
    assert_eq!(
        ledger.health()[0].load_reservations_mb,
        1234,
        "and this run's measurement wins over a smaller stored one"
    );
}

/// A `none`-class load reserves nothing, so it cannot squeeze the windows running
/// concurrently with it.
#[tokio::test]
async fn a_none_class_load_reserves_nothing() {
    let ledger = ledger(10_000, no_margin());
    let none_class = CostDimension {
        unit: CostUnit::None,
        aggregation: None,
        epoch: 1,
        seed_units: None,
        degraded: false,
        canvas_pixels: None,
        max_tokens: None,
    };
    // A neighbour is resident and hungry while the none-class model loads.
    let handle = loaded(Some(1000), Some(0));
    let neighbour = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 9000, 0);
    ledger.ingest_all_for_test();
    let undisturbed = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
    let baseline = undisturbed.grant().mb;
    drop(undisturbed);

    assert!(
        ledger
            .reserve_load_for_test("g/api", none_class, GPU, None)
            .await
            .is_none(),
        "the none class is never reserved for"
    );
    assert_eq!(ledger.health()[0].load_reservations_mb, 0);
    let during = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        during.grant().mb,
        baseline,
        "the neighbour's window is untouched by the concurrent load"
    );
    drop(during);
    // A scaling model on the same GPU still reserves, which is what makes
    // the assertion above about the class rather than about the GPU.
    let charged = ledger
        .reserve_load_for_test("g/b", item_cost(4), GPU, None)
        .await
        .expect("charged");
    assert_eq!(
        ledger.health()[0].load_reservations_mb,
        CONSERVATIVE_BASE_MB
    );
    let squeezed = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert!(
        squeezed.grant().mb < baseline,
        "a scaling load does squeeze: {} vs {baseline}",
        squeezed.grant().mb
    );
    drop(squeezed);
    drop(charged);
}

/// A model whose load reported no device footprint (a remote API, a
/// CPU-fallback impl) needs no reservation on reload.
#[tokio::test]
async fn a_footprintless_model_reserves_nothing_on_reload() {
    let ledger = ledger(10_000, no_margin());
    // First load: nothing is known, so the conservative constant is held.
    let first = ledger
        .reserve_load_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("charged");
    assert_eq!(
        ledger.health()[0].load_reservations_mb,
        CONSERVATIVE_BASE_MB
    );
    drop(first);
    // The load lands and reports no base at all.
    let handle = loaded(None, Some(0));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    assert!(
        ledger
            .reserve_load_for_test("g/a", item_cost(4), GPU, None)
            .await
            .is_none(),
        "a model with no footprint is not reserved for again"
    );
    assert_eq!(ledger.health()[0].load_reservations_mb, 0);
    // A different model on the same GPU is unaffected.
    let other = ledger
        .reserve_load_for_test("g/b", item_cost(4), GPU, None)
        .await
        .expect("charged");
    assert_eq!(
        ledger.health()[0].load_reservations_mb,
        CONSERVATIVE_BASE_MB
    );
    drop(other);
}
