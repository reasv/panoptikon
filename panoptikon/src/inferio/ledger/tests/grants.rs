//! Memory grants: the budget formula, contention, and the pricing margin.
use super::*;

/// Every grant states the model's per-item pixel canvas, from the cost
/// dimension resolved at load.
#[test]
fn a_grant_states_the_models_pixel_canvas() {
    let pixel_cost = |canvas_pixels| CostDimension {
        max_tokens: None,
        unit: CostUnit::Pixel,
        aggregation: Some(CostAggregation::Sum),
        epoch: 1,
        seed_units: Some(2_000_000),
        degraded: false,
        canvas_pixels,
    };
    let ledger = ledger(10_000, VramBudget::default());
    let handle = loaded(Some(1500), Some(1000));
    let admission = ledger
        .register_worker("g/a", pixel_cost(Some(1_835_008)), &handle, None)
        .expect("registers");
    let token = admission
        .request_grant(4_000_000, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().canvas_pixels, Some(1_835_008));
    assert_eq!(token.grant().unit, CostUnit::Pixel);
    // The `issued a memory grant` log line names the same figure, so the
    // canvas a window was priced under can be read from the log.
    assert_eq!(canvas_log_field(token.grant().canvas_pixels), "1835008");
    drop(token);
    drop(admission);

    // Uncapped stays uncapped: the canvas is absent.
    let handle = loaded(Some(1500), Some(1000));
    let admission = ledger
        .register_worker("g/b", pixel_cost(None), &handle, None)
        .expect("registers");
    let token = admission
        .request_grant(4_000_000, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().canvas_pixels, None);
    assert_eq!(canvas_log_field(token.grant().canvas_pixels), "none");
    drop(token);
    drop(admission);

    // An item model has no canvas, and its log line says so.
    let handle = loaded(Some(1500), Some(1000));
    let admission = ledger
        .register_worker("g/c", item_cost(4), &handle, None)
        .expect("registers");
    let token = admission.request_grant(64, None, 1, 0).expect("granted");
    assert_eq!(token.grant().unit, CostUnit::Item);
    assert_eq!(canvas_log_field(token.grant().canvas_pixels), "none");
}

/// The whole formula block on one worker and one GPU.
#[test]
fn formula_block_external_limit_headroom() {
    let ledger = ledger(10_000, VramBudget::default());
    let handle = loaded(Some(1500), Some(1000));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 3000, 1500);
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.footprints_mb, 2000, "1500 base + 500 pool growth");
    assert_eq!(gpu.external_mb, 5000);
    assert!(gpu.external_known);
    assert_eq!(gpu.limit_mb, 4500, "10000 - 5000 * 1.10");
    assert_eq!(gpu.headroom_mb, 2500);
    assert_eq!(gpu.workers.len(), 1);
    drop(admission);
    assert!(
        ledger.health()[0].workers.is_empty(),
        "dropping the admission handle un-charges the replica"
    );
}

/// `cap_fraction` is the server lever: when set, the budget is the min of
/// the two limits. Off (`None`) it never binds.
#[test]
fn cap_fraction_composes_with_margin() {
    let capped = ledger(
        10_000,
        VramBudget {
            margin: Some(DEFAULT_MARGIN),
            cap_fraction: Some(0.5),
            knee_max_bucket_dispersion: None,
        },
    );
    let handle = loaded(Some(1000), Some(0));
    let _a = capped.register_worker("g/a", item_cost(4), &handle, None);
    push_memory(&handle, 4000, 0);
    capped.ingest_all_for_test();
    // external = 10000 - 4000 - 1000 = 5000 -> margin limit 4500;
    // cap limit 5000; min = 4500.
    assert_eq!(capped.health()[0].limit_mb, 4500);

    let tight = ledger(
        10_000,
        VramBudget {
            margin: Some(0.0),
            cap_fraction: Some(0.5),
            knee_max_bucket_dispersion: None,
        },
    );
    let handle = loaded(Some(1000), Some(0));
    let _b = tight.register_worker("g/a", item_cost(4), &handle, None);
    push_memory(&handle, 8000, 0);
    tight.ingest_all_for_test();
    // external = 10000 - 8000 - 1000 = 1000 -> margin-off limit 9000;
    // cap limit 5000; min = 5000.
    assert_eq!(tight.health()[0].limit_mb, 5000);
}

/// A grant is the min of the headroom share, the ramp step and the window's
/// priced content, and it is subtracted from headroom while outstanding.
#[test]
fn grant_is_the_min_rule_and_reserves_headroom() {
    let ledger = ledger(10_000, VramBudget::default());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 9000, 0);
    // 300 MiB, 3 % of the card, are withheld.
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 8700);

    // Pre-fit: the unit budget is the ramp value (seed 4, step 0).
    let token = admission.request_grant(1000, None, 1, 0).expect("granted");
    assert_eq!(token.grant().unit_budget, 4, "the ramp step binds");
    assert_eq!(token.grant().mb, 8700, "pre-fit the MB side is the share");
    assert_eq!(
        ledger.headroom_mb(GPU),
        0,
        "the outstanding grant is subtracted from headroom"
    );
    // With the first grant outstanding nothing is left to price a second
    // window, and a memory-blind pre-fit grant admits one item.
    let blind = admission.request_grant(2, None, 1, 0).expect("granted");
    assert_eq!(blind.grant().mb, 0, "nothing left to price it against");
    assert_eq!(blind.grant().unit_budget, 1, "one item, not the window's 2");
    drop(blind);
    drop(token);
    // With the headroom back, a window smaller than the ramp step binds.
    let smaller = admission.request_grant(2, None, 1, 0).expect("granted");
    assert_eq!(
        smaller.grant().unit_budget,
        2,
        "the priced window content binds"
    );
    drop(smaller);
    assert_eq!(
        ledger.health()[0].grants_outstanding,
        0,
        "dropping a token releases its reservation"
    );
    assert_eq!(ledger.headroom_mb(GPU), 8700);
}

/// Contention: demand first (an idle model is no claimant in the appetite
/// split), then appetite-weighted shares. Pre-fit a share is also at most an
/// equal part of the headroom among the replicas on the GPU.
#[test]
fn contention_splits_by_demand_then_appetite() {
    let ledger = ledger(20_000, no_margin());
    let big = loaded(Some(3000), Some(0));
    let small = loaded(Some(1000), Some(0));
    let a = ledger
        .register_worker("g/big", item_cost(4), &big, None)
        .unwrap();
    let b = ledger
        .register_worker("g/small", item_cost(4), &small, None)
        .unwrap();
    push_memory(&big, 16_000, 0);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 16_000);

    // Only `small` is hungry: no appetite split, so its equal part.
    a.note_demand(0);
    let solo = b.request_grant(u64::MAX, None, 4, 0).unwrap();
    assert_eq!(solo.grant().mb, 8000, "16 000 / 2 replicas");
    drop(solo);

    // Both hungry: shares split 3000:1000 by base weighting (pre-fit).
    a.note_demand(5);
    let smaller = b.request_grant(u64::MAX, None, 4, 0).unwrap();
    assert_eq!(smaller.grant().mb, 4000, "1/4 of the headroom");
    // `b` is now holding that reservation, so it is no longer a claimant:
    // `a` is alone on the 12 000 left and reserves its equal part of it.
    let bigger = a.request_grant(u64::MAX, None, 5, 0).unwrap();
    assert_eq!(bigger.grant().mb, 6000, "12 000 / 2 replicas");
    assert_eq!(
        ledger.headroom_mb(GPU),
        6000,
        "grants never exceed headroom, and a third window still has room"
    );
}

/// A busy replica is not a claimant in the appetite split, because its claim
/// is already subtracted, but it counts in the equal part: pre-fit, the one
/// asking gets half of what is left, not the quarter its appetite would get
/// against the busy one.
#[test]
fn a_busy_replica_counts_in_the_equal_part_but_not_in_the_appetite_split() {
    let ledger = ledger(20_000, no_margin());
    let busy = loaded(Some(3000), Some(0));
    let asking = loaded(Some(1000), Some(0));
    let a = ledger
        .register_worker("g/busy", item_cost(4), &busy, None)
        .unwrap();
    let b = ledger
        .register_worker("g/asking", item_cost(4), &asking, None)
        .unwrap();
    push_memory(&busy, 16_000, 0);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 16_000);
    a.note_demand(3);
    b.note_demand(3);
    // 3/4 by appetite is 12 000; its equal part is 8000.
    let held = a.request_grant(u64::MAX, None, 3, 0).unwrap();
    assert_eq!(held.grant().mb, 8000);
    let asked = b.request_grant(u64::MAX, None, 3, 0).unwrap();
    assert_eq!(
        asked.grant().mb,
        4000,
        "half of the 8000 left, not a quarter of it"
    );
}

/// When even the contention floors oversubscribe headroom they shrink
/// pro-rata; Σ grants never exceeds the headroom they were carved from,
/// and every grant still admits at least one item.
#[test]
fn floors_shrink_pro_rata_when_oversubscribed() {
    let ledger = ledger(5_000, no_margin());
    let mut handles = Vec::new();
    let mut admissions = Vec::new();
    for index in 0..4 {
        let handle = loaded(Some(1100), Some(0));
        let admission = ledger
            .register_worker(&format!("g/m{index}"), item_cost(4), &handle, None)
            .unwrap();
        admission.note_demand(2);
        handles.push(handle);
        admissions.push(admission);
    }
    push_memory(&handles[0], 600, 0);
    ledger.ingest_all_for_test();
    let headroom = ledger.headroom_mb(GPU);
    assert_eq!(headroom, 600, "5000 - 4 * 1100 footprint");
    assert!(
        headroom < SEED_BATCH_FLOOR_MB * 4,
        "the scenario must actually oversubscribe the floors"
    );
    let tokens: Vec<GrantToken> = admissions
        .iter()
        .map(|admission| admission.request_grant(u64::MAX, None, 2, 0).unwrap())
        .collect();
    let granted: u64 = tokens.iter().map(|token| token.grant().mb).sum();
    assert!(
        granted <= headroom,
        "grants never exceed the headroom: {granted} vs {headroom}"
    );
    assert!(
        tokens.iter().all(|token| token.grant().unit_budget >= 1),
        "every grant still admits at least one item"
    );
}

/// Post-fit, a small headroom share converts to units through the slope:
/// the MB side leads and the unit budget follows.
#[test]
fn a_small_share_converts_to_few_units() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let hog = loaded(Some(60_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    let other = ledger
        .register_worker("g/hog", item_cost(4), &hog, None)
        .unwrap();
    push_memory(&handle, 39_000, 0);
    let series: Vec<BatchMeasurement> = (1..=6u64)
        .map(|k| measurement(k * 8, 0, 100 * k * 8))
        .collect();
    handle.lock().unwrap().record_measurements(series);
    clean_window(&admission);
    other.note_demand(9);
    // Headroom is split ~1:60 by base weighting: a few units at 100 MB each.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert!(
        token.grant().unit_budget < 48,
        "the share, not the ratchet, binds: {:?}",
        token.grant()
    );
    assert!(token.grant().unit_budget >= 1);
}

/// An unconfirmed fit (a shipped or fallback-matched profile, or a thin
/// local one) is priced under a widened margin until this machine confirms
/// it with [`LOCAL_CONFIRMATION_SAMPLES`] clean fit samples.
#[test]
fn an_unconfirmed_fit_is_priced_under_a_widened_margin() {
    // Two identical GPUs, differing only in whether the cost is confirmed.
    let grant_mb = |confirmed: bool| -> (u64, f64) {
        let profiles = Arc::new(FakeProfiles {
            seed: confirmed.then(|| ProfileSeed {
                base_mb: 1000,
                // No slope: only the margin differs between the two sides.
                slope_mb_per_unit: 0.0,
                residual_mb: 0.0,
                samples: 0,
                knee_units: None,
                knee_trials: Default::default(),
                knee_rates: Vec::new(),
                local: true,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: LOCAL_CONFIRMATION_SAMPLES,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        // A configured margin, so the default rule's 1 GiB reserve cap does
        // not hide the widening on a GPU holding 49 GB of external usage.
        let ledger = ledger_with(100_000, user_margin(DEFAULT_MARGIN), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        // Something else holds 49 GB, so the margin has something to bite on.
        push_memory(&handle, 50_000, 0);
        ledger.ingest_all_for_test();
        let margin = ledger.health()[0].workers[0].effective_margin;
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        (token.grant().mb, margin)
    };
    let (unconfirmed_mb, unconfirmed_margin) = grant_mb(false);
    let (confirmed_mb, confirmed_margin) = grant_mb(true);
    assert_eq!(
        unconfirmed_margin,
        DEFAULT_MARGIN + UNCONFIRMED_MARGIN_BONUS,
        "nothing local stands behind this model yet"
    );
    assert_eq!(confirmed_margin, DEFAULT_MARGIN);
    assert!(
        confirmed_mb > unconfirmed_mb,
        "the widened margin costs the unconfirmed model headroom: \
         {unconfirmed_mb} vs {confirmed_mb}"
    );

    // Five clean measured windows confirm the fit and drop the widening.
    let ledger = ledger(100_000, user_margin(DEFAULT_MARGIN));
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 50_000, 0);
    for units in [4, 8, 16, 32] {
        measured_window(&handle, &admission, units);
        assert_eq!(
            ledger.health()[0].workers[0].effective_margin,
            DEFAULT_MARGIN + UNCONFIRMED_MARGIN_BONUS,
            "still under the confirmation count"
        );
    }
    measured_window(&handle, &admission, 64);
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.local_samples, LOCAL_CONFIRMATION_SAMPLES);
    assert_eq!(
        worker.effective_margin, DEFAULT_MARGIN,
        "confirmed by local evidence, so the widening drops"
    );
}

/// The reserve rule: an **unset** margin gets the default fraction capped at
/// [`DEFAULT_RESERVE_CAP_MB`], so the last gigabytes of a busy GPU stay
/// usable, and at least 3 % of the card; a margin the user wrote down is
/// honoured verbatim and uncapped.
#[test]
fn the_reserve_is_capped_only_under_an_unset_margin() {
    // 97 887 MiB of GPU, 1 000 of it ours.
    // (label, budget, free reading, external, reserve, rule, headroom left)
    for (label, budget, free_mb, external, reserve, rule, priced) in [
        (
            "the fraction would have withheld 8 889 MiB: the regime where \
             `external × 1.1` used to reach the total and leave a limit of 0",
            VramBudget::default(),
            8_000,
            88_887,
            DEFAULT_RESERVE_CAP_MB,
            RESERVE_RULE_CAPPED_DEFAULT,
            true,
        ),
        (
            "a margin the user wrote down is applied to the MiB, uncapped: \
             total − ceil(external × 1.1)",
            user_margin(DEFAULT_MARGIN),
            8_000,
            88_887,
            8_889,
            RESERVE_RULE_USER_MARGIN,
            false,
        ),
        (
            "on a quiet GPU ceil(4 000 × 0.10) = 400 is under 3 % of the \
             card, which is itself capped",
            VramBudget::default(),
            92_887,
            4_000,
            DEFAULT_RESERVE_CAP_MB,
            RESERVE_RULE_GPU_FLOOR,
            true,
        ),
    ] {
        let ledger = ledger(97_887, budget);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, free_mb, 0);
        ledger.ingest_all_for_test();

        let gpu = &ledger.health()[0];
        assert_eq!(gpu.external_mb, external, "{label}");
        assert_eq!(gpu.reserve_mb, reserve, "{label}");
        assert_eq!(gpu.reserve_rule, rule, "{label}");
        assert_eq!(gpu.limit_mb, 97_887 - external - reserve, "{label}");
        assert_eq!(gpu.margin, DEFAULT_MARGIN, "{label}");
        if priced {
            // The GPU still has room, so a grant on it is priced.
            assert!(gpu.headroom_mb > 0, "{label}");
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            assert!(
                token.grant().mb > 0,
                "{label}: an `mb = 0` grant is priced against nothing"
            );
        }
    }
}

/// Under an unset margin a GPU with memory of its own reserves at least 3 %
/// of the card, itself at most [`DEFAULT_RESERVE_CAP_MB`]: on a card with
/// little other usage the default fraction reserves almost nothing. Once
/// another process holds more than 30 % of the card the default fraction is
/// the larger and nothing changes. A margin the user wrote still applies as
/// written, and the CPU device and Apple's unified memory keep their own
/// rules. A unified-memory GPU on Linux is a GPU like any other here.
#[test]
fn an_unset_margin_reserves_at_least_three_percent_of_a_gpu() {
    let unset = VramBudget::default();
    // (card, budget, other usage, reserve, rule)
    for (total, budget, external, reserve, rule) in [
        (8_192, unset, 165, 245, RESERVE_RULE_GPU_FLOOR),
        (16_368, unset, 165, 491, RESERVE_RULE_GPU_FLOOR),
        (24_576, unset, 165, 737, RESERVE_RULE_GPU_FLOOR),
        (97_887, unset, 165, 1024, RESERVE_RULE_GPU_FLOOR),
        // A quarter of the card: 205 by the fraction, less than the 245.
        (8_192, unset, 2_048, 245, RESERVE_RULE_GPU_FLOOR),
        // From 30 % of the card on: the fraction, capped, as before.
        (8_192, unset, 2_450, 245, RESERVE_RULE_CAPPED_DEFAULT),
        (8_192, unset, 2_458, 246, RESERVE_RULE_CAPPED_DEFAULT),
        (16_368, unset, 8_184, 819, RESERVE_RULE_CAPPED_DEFAULT),
        (24_576, unset, 12_288, 1024, RESERVE_RULE_CAPPED_DEFAULT),
        (97_887, unset, 40_000, 1024, RESERVE_RULE_CAPPED_DEFAULT),
        (
            16_368,
            user_margin(DEFAULT_MARGIN),
            165,
            17,
            RESERVE_RULE_USER_MARGIN,
        ),
        (16_368, user_margin(0.0), 165, 0, RESERVE_RULE_USER_MARGIN),
    ] {
        let ledger = ledger(total, budget);
        let margin = ledger.budgets.for_gpu(GPU).margin_in_force();
        let state = ledger.lock();
        assert_eq!(
            ledger.reserve_locked(&state, GPU, external, margin),
            (reserve, rule),
            "{total} MiB card, {external} MiB of other usage"
        );
    }

    // The CPU device keeps its RAM floor. A GPU carved out of host RAM has
    // the floor on Linux, and on a Mac the fraction alone.
    let host = VramLedger::for_test(
        &[
            (GPU, "TEST 9000", 24_576),
            (super::cpu::DEVICE_KEY, "CPU", 65_536),
        ],
        unset,
    );
    host.lock().gpus.get_mut(GPU).unwrap().unified_ram_mb = Some(65_536);
    for (metal, device, expected) in [
        (
            false,
            super::cpu::DEVICE_KEY,
            (6_553, RESERVE_RULE_RAM_FLOOR),
        ),
        (false, GPU, (737, RESERVE_RULE_GPU_FLOOR)),
        (true, GPU, (17, RESERVE_RULE_CAPPED_DEFAULT)),
    ] {
        let mut state = host.lock();
        state.metal_allocator = metal;
        assert_eq!(
            host.reserve_locked(&state, device, 165, DEFAULT_MARGIN),
            expected
        );
    }

    // `/health` names the rule and prices the limit under it.
    let ledger = ledger(16_368, unset);
    let handle = loaded(Some(554), Some(0));
    let _admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 16_368 - 554 - 165, 0);
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert_eq!((gpu.external_mb, gpu.reserve_mb), (165, 491));
    assert_eq!(gpu.reserve_rule, "gpu_floor");
    assert_eq!(gpu.limit_mb, 16_368 - 165 - 491);
}

/// Where a full CUDA GPU spills to system RAM, an unset margin reserves the
/// cap itself, whatever other processes use. A margin the user wrote, for all
/// GPUs or one, still applies uncapped; the CPU device keeps its own floor;
/// a margin of 0 reserves nothing on a GPU. Another GPU on the same host that
/// fails the allocation keeps the card floor.
#[test]
fn a_spilling_gpu_reserves_the_cap_under_an_unset_margin() {
    const OTHER: &str = "GPU-bbbb";
    let spilling = |budgets: VramBudgets| VramBudgets {
        spilling: HashSet::from([GPU.to_owned()]),
        ..budgets
    };
    let devices = [
        (GPU, "TEST 9000", 24_576),
        (OTHER, "TEST 9000", 24_576),
        (super::cpu::DEVICE_KEY, "CPU", 65_536),
    ];
    // (label, budgets, device, external, reserve, rule)
    for (label, budgets, device, external, reserve, rule) in [
        (
            "unset: the cap, not ceil(4 000 × 0.10) = 400",
            spilling(VramBudget::default().into()),
            GPU,
            4_000,
            DEFAULT_RESERVE_CAP_MB,
            RESERVE_RULE_FLAT_DEFAULT,
        ),
        (
            "unset, with no other process on the GPU",
            spilling(VramBudget::default().into()),
            GPU,
            0,
            DEFAULT_RESERVE_CAP_MB,
            RESERVE_RULE_FLAT_DEFAULT,
        ),
        (
            "a margin for every GPU",
            spilling(user_margin(DEFAULT_MARGIN).into()),
            GPU,
            4_000,
            400,
            RESERVE_RULE_USER_MARGIN,
        ),
        (
            "a margin for this GPU alone",
            spilling(VramBudgets::default().with_gpu(GPU, user_margin(0.0))),
            GPU,
            4_000,
            0,
            RESERVE_RULE_USER_MARGIN,
        ),
        (
            "the CPU device",
            spilling(VramBudget::default().into()),
            super::cpu::DEVICE_KEY,
            4_000,
            6_553,
            RESERVE_RULE_RAM_FLOOR,
        ),
        (
            "a GPU that fails the allocation instead: 3 % of the card",
            spilling(VramBudget::default().into()),
            OTHER,
            4_000,
            737,
            RESERVE_RULE_GPU_FLOOR,
        ),
    ] {
        let ledger = VramLedger::for_test(&devices, budgets);
        let margin = ledger.budgets.for_gpu(device).margin_in_force();
        let state = ledger.lock();
        assert_eq!(
            ledger.reserve_locked(&state, device, external, margin),
            (reserve, rule),
            "{label}"
        );
        let floor = if device == super::cpu::DEVICE_KEY {
            reserve
        } else {
            0
        };
        assert_eq!(
            ledger.reserve_locked(&state, device, external, 0.0).0,
            floor,
            "{label}: a margin of 0"
        );
    }

    // `/health` names the rule and prices the limit under it.
    let ledger = VramLedger::for_test(
        &[(GPU, "TEST 9000", 24_576)],
        spilling(VramBudget::default().into()),
    );
    let handle = loaded(Some(1000), Some(0));
    let _admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 20_000, 0);
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.external_mb, 3_576);
    assert_eq!(gpu.reserve_mb, DEFAULT_RESERVE_CAP_MB);
    assert_eq!(gpu.reserve_rule, "flat_default");
    assert_eq!(gpu.limit_mb, 24_576 - 3_576 - DEFAULT_RESERVE_CAP_MB);
}

/// A degraded cost dimension (no parseable `metadata.cost`) widens the same
/// way, and permanently.
#[test]
fn a_degraded_cost_dimension_widens_the_margin_permanently() {
    let ledger = ledger(100_000, VramBudget::default());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", CostDimension::fallback(), &handle, None)
        .unwrap();
    push_memory(&handle, 50_000, 0);
    for units in [4, 8, 16, 32, 64] {
        measured_window(&handle, &admission, units);
    }
    let worker = &ledger.health()[0].workers[0];
    assert!(worker.local_samples >= LOCAL_CONFIRMATION_SAMPLES);
    assert_eq!(
        worker.effective_margin,
        DEFAULT_MARGIN + UNCONFIRMED_MARGIN_BONUS,
        "local samples cannot confirm a dimension that was never declared"
    );
}

/// Scatter widens the margin too, in proportion to the model's own base, and
/// clamped.
#[test]
fn a_scattered_fit_widens_the_margin() {
    let ledger = ledger(100_000, VramBudget::default());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 50_000, 0);
    // A scattered fit series: residual ~150 MB against a 1000 MB base.
    let series: Vec<BatchMeasurement> = (1..=8u64)
        .map(|k| {
            measurement(
                k * 8,
                0,
                10 * k * 8 + if k.is_multiple_of(2) { 300 } else { 0 },
            )
        })
        .collect();
    handle.lock().unwrap().record_measurements(series);
    clean_window(&admission);
    let worker = &ledger.health()[0].workers[0];
    let residual = worker.fit.as_ref().unwrap().residual_mb;
    assert!(
        residual > 50.0,
        "the series really is scattered: {residual}"
    );
    assert!(
        worker.effective_margin > DEFAULT_MARGIN,
        "and that scatter reaches the margin: {}",
        worker.effective_margin
    );
    assert!(
        worker.effective_margin <= DEFAULT_MARGIN + MAX_MARGIN_INCREMENT,
        "clamped: {}",
        worker.effective_margin
    );
}

/// The widening is **additive**, and only its own increment is clamped: a
/// configured margin survives whatever the user wrote, and `margin = 0`
/// still gets the unconfirmed bonus.
#[test]
fn margin_widening_is_additive_and_never_clamps_the_configured_margin() {
    // A margin far above 0.5, through `/health` and a real grant request.
    let ledger = ledger(
        100_000,
        VramBudget {
            margin: Some(0.9),
            cap_fraction: None,
            knee_max_bucket_dispersion: None,
        },
    );
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    ledger.ingest_all_for_test();
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.effective_margin,
        0.9 + UNCONFIRMED_MARGIN_BONUS,
        "the user's margin is honoured whole and widened on top"
    );
    assert!(
        admission.request_grant(u64::MAX, None, 1, 0).is_some(),
        "and pricing a window under it does not panic"
    );

    // Zero: a multiplicative widening would leave no protection at all.
    let unmargined = self::ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let _admission = unmargined
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    unmargined.ingest_all_for_test();
    assert_eq!(
        unmargined.health()[0].workers[0].effective_margin,
        UNCONFIRMED_MARGIN_BONUS,
        "margin = 0 still widens for an unconfirmed fit"
    );
}

/// A grant and the pool growth it produces are the **same memory**: a
/// post-fit grant's MB figure is the envelope over `reserved_at_load` that
/// the footprint's growth term counts once the pool has grown into it.
#[test]
fn a_grant_and_the_pool_it_grew_are_charged_once() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    // 100 MB/unit, so a 24-unit batch prices at 2400 MB.
    let series: Vec<BatchMeasurement> = (1..=6u64)
        .map(|k| measurement(k * 4, 0, 100 * k * 4))
        .collect();
    handle.lock().unwrap().record_measurements(series);
    push_memory(&handle, 90_000, 2400);
    clean_window(&admission);
    admission.earn_next_size();
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.reserved_mb, Some(2400));
    assert_eq!(worker.footprint_mb, 3400, "1000 base + 2400 pool growth");

    let token = admission.request_grant(24, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 24);
    assert_eq!(token.grant().mb, 2400, "24 units at 100 MB each");
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.charge_mb, 3400,
        "the grant reaches no further than the pool already held: charged \
         2400 over base, not 4800"
    );
    assert_eq!(ledger.health()[0].charges_mb, 3400);
}

/// A 6 GB card and a model with a 2.4 GB working set: the share is not zero.
#[test]
fn a_small_card_does_not_collapse_to_a_zero_share() {
    let ledger = ledger(6144, no_margin());
    let handle = loaded(Some(1200), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    let series: Vec<BatchMeasurement> = (1..=6u64)
        .map(|k| measurement(k * 4, 0, 100 * k * 4))
        .collect();
    handle.lock().unwrap().record_measurements(series);
    // free = 6144 - 1200 base - 2400 pool = 2544, so external is 0.
    push_memory(&handle, 2544, 2400);
    clean_window(&admission);
    admission.earn_next_size();
    assert_eq!(ledger.health()[0].external_mb, 0);
    let first = admission.request_grant(24, None, 1, 0).unwrap();
    assert_eq!(first.grant().mb, 2400);
    drop(first);
    // A second window is priced against a GPU that is *not* full.
    let second = admission.request_grant(24, None, 1, 0).unwrap();
    assert!(
        second.grant().unit_budget >= 24,
        "the working set is not charged twice: {:?}",
        second.grant()
    );
}

/// A zero share is charged as zero MB and admits one item, never the seed
/// batch it cannot pay for.
#[test]
fn a_zero_share_grants_zero_mb_and_admits_one_unit() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(10_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 0, 0);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 0, "the GPU is full");
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().mb, 0, "nothing was reserved, and it says so");
    assert_eq!(
        token.grant().unit_budget,
        1,
        "a memory-blind window is one item, not the whole seed batch"
    );
}

/// A window's own requests stop counting as demand when it settles.
#[test]
fn a_settled_window_retires_its_own_demand() {
    let ledger = ledger(20_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 18_000, 0);
    ledger.ingest_all_for_test();
    // 3 requests in the window, 2 still queued behind it.
    let token = admission.request_grant(u64::MAX, None, 3, 2).unwrap();
    assert_eq!(ledger.health()[0].workers[0].pending_requests, 5);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].workers[0].pending_requests,
        2,
        "the window's own three are done; the queue behind it is still demand"
    );
}

/// Budgets are keyed by GPU **instance**: two identical GPUs share their
/// calibration profile but carry different admission limits.
#[test]
fn budgets_resolve_per_gpu() {
    const A: &str = "GPU-aaaa";
    const B: &str = "GPU-bbbb";
    let budgets = VramBudgets::uniform(VramBudget {
        margin: Some(0.0),
        cap_fraction: None,
        knee_max_bucket_dispersion: None,
    })
    .with_gpu(
        B,
        VramBudget {
            margin: Some(0.0),
            cap_fraction: Some(0.5),
            knee_max_bucket_dispersion: None,
        },
    );
    let ledger = VramLedger::for_test(
        &[(A, "TEST 9000", 10_000), (B, "TEST 9000", 10_000)],
        budgets,
    );
    let on_a = loaded_on(A, Some(1000), Some(0));
    let on_b = loaded_on(B, Some(1000), Some(0));
    let _a = ledger
        .register_worker("g/a", item_cost(4), &on_a, None)
        .unwrap();
    let _b = ledger
        .register_worker("g/b", item_cost(4), &on_b, None)
        .unwrap();
    push_memory(&on_a, 9000, 0);
    push_memory(&on_b, 9000, 0);
    ledger.ingest_all_for_test();

    let gpus = ledger.health();
    let a = gpus.iter().find(|gpu| gpu.gpu_uuid == A).unwrap();
    let b = gpus.iter().find(|gpu| gpu.gpu_uuid == B).unwrap();
    // Both GPUs: external = 10000 - 9000 - 1000 = 0, margin 0.
    assert_eq!(a.limit_mb, 10_000, "no cap on this GPU");
    assert_eq!(b.limit_mb, 5000, "the per-GPU cap_fraction binds");
    assert_eq!(a.cap_fraction, None);
    assert_eq!(b.cap_fraction, Some(0.5));
    assert_eq!(a.headroom_mb, 9000);
    assert_eq!(b.headroom_mb, 4000);
}

/// The margin half of the same rule reaches the per-model effective margin:
/// a GPU's configured margin is the base every widening is added to.
#[test]
fn per_gpu_margins_reach_the_effective_margin() {
    const A: &str = "GPU-aaaa";
    const B: &str = "GPU-bbbb";
    let budgets = VramBudgets::uniform(VramBudget {
        margin: Some(0.0),
        cap_fraction: None,
        knee_max_bucket_dispersion: None,
    })
    .with_gpu(
        B,
        VramBudget {
            margin: Some(0.5),
            cap_fraction: None,
            knee_max_bucket_dispersion: None,
        },
    );
    let ledger = VramLedger::for_test(
        &[(A, "TEST 9000", 10_000), (B, "TEST 9000", 10_000)],
        budgets,
    );
    let on_a = loaded_on(A, Some(1000), Some(0));
    let on_b = loaded_on(B, Some(1000), Some(0));
    let _a = ledger
        .register_worker("g/a", item_cost(4), &on_a, None)
        .unwrap();
    let _b = ledger
        .register_worker("g/b", item_cost(4), &on_b, None)
        .unwrap();
    // external = 10000 - 5000 - 1000 = 4000 on both GPUs.
    push_memory(&on_a, 5000, 0);
    push_memory(&on_b, 5000, 0);
    ledger.ingest_all_for_test();

    let gpus = ledger.health();
    let a = gpus.iter().find(|gpu| gpu.gpu_uuid == A).unwrap();
    let b = gpus.iter().find(|gpu| gpu.gpu_uuid == B).unwrap();
    assert_eq!(a.margin, 0.0);
    assert_eq!(b.margin, 0.5);
    assert_eq!(a.limit_mb, 6000, "10000 - 4000: external, uninflated");
    assert_eq!(b.limit_mb, 4000, "10000 - 4000 * 1.5");
    // Both unconfirmed: the same increment on their own GPU's margin.
    assert_eq!(a.workers[0].effective_margin, UNCONFIRMED_MARGIN_BONUS);
    assert_eq!(
        b.workers[0].effective_margin,
        0.5 + UNCONFIRMED_MARGIN_BONUS
    );
}

/// When the sole resident's footprint has passed the GPU's limit, `headroom`
/// saturates at 0, but a grant spent in its own pool adds nothing to
/// [`WorkerEntry::charge_mb`]. It is granted that room; the neighbour is
/// granted none of it.
#[test]
fn a_resident_is_granted_the_pool_its_own_footprint_already_paid_for() {
    let ledger = ledger(10_000, no_margin());
    let pinned = loaded(Some(1000), Some(0));
    let pinned_admission = ledger
        .register_worker("g/pinned", item_cost(4), &pinned, None)
        .unwrap();
    let neighbour = loaded(Some(200), Some(0));
    let neighbour_admission = ledger
        .register_worker("g/neighbour", item_cost(4), &neighbour, None)
        .unwrap();
    neighbour_admission.note_demand(1);
    // Charges 9500 + 200 = 9700 on a card whose external tenant is
    // 10000 - 0 - 9700 = 300; the pre-fit margin bonus reserves 45 of that,
    // so limit = 9655 and the GPU is 45 MiB over its own limit.
    push_memory(&pinned, 0, 8500);
    push_memory(&neighbour, 0, 0);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].headroom_mb, 0);

    let token = pinned_admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        token.grant().mb,
        8455,
        "its own 8500 MiB of pool, less the 45 the card is over by"
    );
    assert!(!token.grant().squeezed, "this is not a memory squeeze");
    assert!(
        ledger.take_pending_trims().is_empty(),
        "and nothing has to be released for it"
    );

    let neighbours = neighbour_admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        neighbours.grant().mb,
        0,
        "the pool credited above is the pinned replica's own"
    );
    drop(neighbours);
    drop(token);
}

/// The limit a pre-fit window is priced under: the GPU's own limit less the
/// unconfirmed-fit margin bonus on the external reading.
fn effective_limit(ledger: &Arc<VramLedger>, total_mb: u64) -> u64 {
    let health = ledger.health();
    let limit = health[0].limit_mb;
    let external = total_mb - limit;
    limit - ((external as f64) * UNCONFIRMED_MARGIN_BONUS).ceil() as u64
}

fn charges_now(ledger: &Arc<VramLedger>) -> u64 {
    ledger.health()[0].charges_mb
}

/// Sole claimant, both branches of [`WorkerEntry::charge_mb`], by hand: a
/// grant keeps `Σ charges after <= max(effective limit, Σ charges before)`.
#[test]
fn a_sole_claimants_grant_keeps_the_charge_invariant_in_both_branches() {
    // (a) grants below pool growth: 1000 base + 8500 pool, free 0.
    // external = 10000 - 0 - 9500 = 500; limit = 9500; bonus reserve
    // ceil(500*0.15) = 75; limit_eff = 9425; headroom = 9425 - 9500 = -75;
    // credit = 8500 - 0; own_room = 8425.
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/pinned", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 0, 8500);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].headroom_mb, 0);
    let limit_eff = effective_limit(&ledger, 10_000);
    assert_eq!(limit_eff, 9425);
    let charges_before = charges_now(&ledger);
    assert_eq!(charges_before, 9500);

    let first = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(first.grant().mb, 8425, "limit_eff - base - own grants");
    assert_eq!(
        charges_now(&ledger),
        9500,
        "spent inside the pool: charge = base + max(8500, 8425)"
    );
    assert!(charges_now(&ledger) <= charges_before.max(limit_eff));

    // (b) a second grant while the first is outstanding: credit is now
    // 8500 - 8425 = 75 and own_room = -75 + 75 = 0, so a blind grant.
    let second = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        second.grant().mb,
        0,
        "own_room = 0 without the limit being under the base"
    );
    assert_eq!(charges_now(&ledger), 9500);
    drop(second);
    drop(first);
}

/// The other branch of `charge_mb`: outstanding grants already past the
/// pool, where the credit is zero and the share is the plain headroom.
#[test]
fn a_requester_whose_grants_pass_its_pool_is_credited_nothing() {
    // 1000 base + 300 pool, free 5000. external = 10000 - 5000 - 1300 =
    // 3700; limit = 6300; bonus 555; limit_eff = 5745; charges 1300;
    // headroom 4445; credit 300; own_room 4745.
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/one", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 5000, 300);
    ledger.ingest_all_for_test();
    let limit_eff = effective_limit(&ledger, 10_000);
    assert_eq!(limit_eff, 5745);

    let first = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(first.grant().mb, 4745);
    assert_eq!(
        charges_now(&ledger),
        5745,
        "grants past the pool: charge = base + grants, exactly the limit"
    );
    assert!(charges_now(&ledger) <= limit_eff, "never past the limit");

    let second = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(second.grant().mb, 0, "credit = max(0, 300 - 4745) = 0");
    assert_eq!(charges_now(&ledger), 5745);
    drop(second);
    drop(first);
}

/// The two-claimant split: the credit is added after the division, and the
/// requester's share becomes a real charge, so the neighbour's headroom
/// falls by exactly that share.
#[test]
fn a_split_adds_the_credit_after_the_division_and_still_fits() {
    // R: 500 base + 4000 pool. N: 1000 base, no pool. free 2000.
    // external = 10000 - 2000 - 5500 = 2500; limit = 7500; bonus 375;
    // limit_eff = 7125; charges 5500; headroom 1625; credit(R) 4000;
    // own_room(R) 5625. Appetites pre-fit are the bases: 500 and 1000, so
    // R's share = floor(1625 * 500/1500) = 541 (floors 256 each fit, and it
    // is under R's equal part, 1625 / 2).
    let ledger = ledger(10_000, no_margin());
    let pooled = loaded(Some(500), Some(0));
    let pooled_admission = ledger
        .register_worker("g/pooled", item_cost(4), &pooled, None)
        .unwrap();
    let other = loaded(Some(1000), Some(0));
    let other_admission = ledger
        .register_worker("g/other", item_cost(4), &other, None)
        .unwrap();
    pooled_admission.note_demand(1);
    other_admission.note_demand(1);
    push_memory(&pooled, 2000, 4000);
    push_memory(&other, 2000, 0);
    ledger.ingest_all_for_test();
    let limit_eff = effective_limit(&ledger, 10_000);
    assert_eq!(limit_eff, 7125);
    assert_eq!(charges_now(&ledger), 5500);
    assert_eq!(ledger.health()[0].headroom_mb, 2000, "GPU-wide, no bonus");

    let held = pooled_admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(held.grant().mb, 4541, "541 of headroom + 4000 of own pool");
    assert_eq!(
        charges_now(&ledger),
        6041,
        "500 + max(4000, 4541) + 1000: the share landed as a real charge"
    );

    let neighbour = other_admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        neighbour.grant().mb,
        1084,
        "what is left of the effective limit, and no more"
    );
    assert_eq!(charges_now(&ledger), 7125, "Σ charges == limit_eff exactly");
    assert!(charges_now(&ledger) <= limit_eff);
    drop(neighbour);
    drop(held);
}

/// A load reservation is subtracted before the credit is added, so a
/// requester cannot spend a reservation's memory out of its own pool.
#[tokio::test]
async fn the_credit_does_not_reach_past_a_load_reservation() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/one", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 5000, 300);
    ledger.ingest_all_for_test();
    let limit_eff = effective_limit(&ledger, 10_000);
    assert_eq!(limit_eff, 5745);
    let reservation = ledger
        .reserve_load_for_test("g/two", item_cost(4), GPU, None)
        .await
        .expect("known GPU");
    let reserved = ledger.health()[0].load_reservations_mb;
    assert!(reserved > 0);

    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        token.grant().mb,
        4745 - reserved,
        "the reservation comes off the headroom before the credit"
    );
    assert!(
        charges_now(&ledger) + reserved <= limit_eff,
        "charges + reservations still inside the limit"
    );
    drop(token);
    drop(reservation);
}
