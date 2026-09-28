//! Stored profiles: what a seed confers, and what is written back.
use super::*;

/// A **shipped** profile confers its anchor exactly as a local one does: the
/// first window opens at the ramp floor it implies, growth is capped at
/// `RATCHET_FACTOR x` it, and it confers no local confirmation or sample ring.
#[test]
fn a_shipped_profiles_anchor_floors_the_ramp_and_caps_growth() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: None,
            local: false,
            fit_is_local: false,
            exact_torch: true,
            max_units_measured: 512,
            local_samples: 99,
            knee_clean_windows: 0,
            ring: vec![FitSample {
                units: 512,
                delta_mb: 5_120,
            }],
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    ledger.ingest_all_for_test();

    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.max_units_measured, 512,
        "the anchor travels: the card name is not a gate on it"
    );
    assert_eq!(worker.local_samples, 0, "but it confers no confirmation");
    assert!(
        (worker.fit.as_ref().unwrap().slope_mb_per_unit - 10.0).abs() < 1e-9,
        "and its fit prices the very first window"
    );
    assert_eq!(
        ledger.calibration_state("g/a", GPU).unwrap().samples.len(),
        0,
        "and its samples are not this machine's evidence"
    );
    assert_eq!(
        measured_window(&handle, &admission, 512),
        512,
        "the first window opens at the ramp floor for 512, not at the seed"
    );
    // A window whose content was small: the measured range does not extend,
    // so the ceiling stays where the seeded anchor put it.
    assert_eq!(measured_window(&handle, &admission, 8), 1_024);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        512 * RATCHET_FACTOR,
        "growth stops at RATCHET_FACTOR x the anchor"
    );
}

/// A seeded anchor is somebody else's measurement and never travels into the
/// local store under our generator stamp.
#[test]
fn a_seeded_anchor_is_never_written_back_as_this_machines_own() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: None,
            local: false,
            fit_is_local: false,
            exact_torch: true,
            max_units_measured: 512,
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
    // A window granted at the seeded anchor whose *content* was 8 units:
    // local evidence never reaches 512, so the anchor stays a claim.
    measured_window(&handle, &admission, 8);
    assert_eq!(ledger.health()[0].workers[0].max_units_measured, 512);
    let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        written.max_units_measured, 0,
        "the store is told nothing about an anchor this machine never ran"
    );
    assert_eq!(written.local_samples, 1, "only the sample it did measure");

    // And once a batch that size does run here, the same number is written
    // as this machine's own.
    measured_window(&handle, &admission, 512);
    let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(written.max_units_measured, 512);
}

/// A card whose headroom stops it short of the conferred anchor stores what
/// it did measure: 295 units under a shipped 3 072.
#[test]
fn a_host_that_cannot_reach_a_conferred_anchor_records_what_it_ran() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(3072, false)),
        ..FakeProfiles::default()
    });
    // At 10 MiB/unit the anchor's 30 720 MiB is out of reach here, so the
    // window is squeezed to what this card's headroom affords.
    let ledger = ledger_with(4_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 3_000, 0);
    ledger.ingest_all_for_test();

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let granted = token.grant().unit_budget;
    assert!(
        (64..3072).contains(&granted),
        "the headroom, not the anchor, sized this window: {granted}"
    );
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(granted, 0, 10 * granted + 100)]);
    token.finish(WindowOutcome::Responded { oom: None });

    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        3072,
        "the seeded anchor still floors the ramp and caps growth"
    );
    let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        written.max_units_measured, granted,
        "and the store is told the batch this GPU ran, never the claim"
    );
}

/// On the next start that figure is adopted, floors the ramp at the largest
/// step at or below it, and stays seeded until a clean batch here reaches it.
#[test]
fn a_locally_recorded_anchor_floors_the_next_starts_ramp() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(295, true)),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    ledger.ingest_all_for_test();
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        295,
        "the local row's anchor is adopted"
    );
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        256,
        "and the first window opens at the step it implies"
    );
}

/// A clean batch that ran small for want of work is no measurement of this
/// card: a 64-unit tail inside a 512-unit grant leaves the store alone.
#[test]
fn a_batch_that_did_not_spend_its_budget_records_no_anchor() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(512, false)),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    ledger.ingest_all_for_test();

    assert_eq!(measured_window(&handle, &admission, 64), 512);
    let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        written.max_units_measured, 0,
        "64 of a 512-unit budget measures the queue, not the card"
    );
    assert_eq!(written.local_samples, 1, "only the sample it did measure");
}

/// Under a seeded anchor an out-of-memory window halves it, where an anchor a
/// clean batch on this GPU reached would survive, even when the seed came
/// from this machine's own store file.
#[test]
fn an_oom_halves_a_seeded_anchor_but_not_a_measured_one() {
    let seed = || {
        Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: None,
                local: true,
                fit_is_local: true,
                exact_torch: true,
                max_units_measured: 512,
                local_samples: 0,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        })
    };
    for (ran_it_here, expected) in [(false, 256), (true, 512)] {
        let profiles = seed();
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 512);
        if ran_it_here {
            measured_window(&handle, &admission, 512);
        }

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            expected,
            "ran it here = {ran_it_here}"
        );
    }
}

/// A window that ran one clean batch at the seeded anchor and then went out of
/// memory cannot confirm it: the backstop still halves it and nothing is stored.
#[test]
fn a_clean_batch_in_a_failed_window_never_confirms_the_anchor() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(512, false)),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].workers[0].max_units_measured, 512);

    // The pool fit one batch at 512 by luck; the window then reported an
    // out-of-memory error frame.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(512, 0, 5_220)]);
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        256,
        "the lucky batch does not make the size that failed this GPU's own"
    );
    let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        written.max_units_measured, 0,
        "and nothing about it travels into the local store"
    );
}

/// The backstop's three triggers, and the cancelled window, which reports no
/// failure at all.
#[test]
fn every_out_of_memory_lowers_a_seeded_anchor_and_a_cancelled_window_does_not() {
    for (outcome, expected) in [
        (WindowOutcome::WorkerDied, 256),
        (WindowOutcome::Aborted, 512),
        (
            WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            },
            256,
        ),
    ] {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(512, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(outcome);
        assert_eq!(
            ledger
                .calibration_state("g/a", GPU)
                .unwrap()
                .max_units_measured,
            expected,
            "outcome = {outcome:?}"
        );
    }
}

/// An anchor with no fit under it confers nothing: with no slope to turn it
/// into MB, the ramp starts from the seed.
#[test]
fn an_anchor_without_a_fit_confers_nothing() {
    let mut seed = seeded_anchor(3072, false);
    // What `pending_update_locked` writes until the ring reaches
    // MIN_FIT_SAMPLES.
    seed.slope_mb_per_unit = 0.0;
    seed.samples = 0;
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seed),
        ..FakeProfiles::default()
    });
    // A 12 GB card.
    let ledger = ledger_with(12_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 11_000, 0);
    ledger.ingest_all_for_test();
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        0,
        "nothing adopted it"
    );
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        64,
        "the seed's own ramp, not a batch nothing here can price"
    );
}

/// The conferred anchor is also the contention weight, so it is clamped by
/// what the card affords rather than taken from the neighbour's slice.
#[test]
fn a_conferred_anchor_buys_no_appetite_this_card_cannot_run() {
    let profiles = Arc::new(FakeProfiles {
        // 3 072 units at 12.5 MiB each is 38 GB of batch — on a 12 GB card.
        seed: Some(seeded_anchor(3072, false)),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(12_000, no_margin(), &profiles);
    let mine = loaded(Some(1000), Some(0));
    let theirs = loaded(Some(1000), Some(0));
    let a = ledger
        .register_worker("g/a", item_cost(64), &mine, None)
        .unwrap();
    let b = ledger
        .register_worker("g/b", item_cost(64), &theirs, None)
        .unwrap();
    push_memory(&mine, 10_000, 0);
    ledger.ingest_all_for_test();
    // The neighbour's anchor is what this card actually affords.
    {
        let mut state = ledger.lock();
        state
            .calibration
            .get_mut(&("g/b".to_owned(), GPU.to_owned()))
            .expect("seeded")
            .max_units_measured = 960;
    }
    {
        let state = ledger.lock();
        let appetite = |model: &str| {
            let entry = state
                .workers
                .values()
                .find(|entry| entry.inference_id == model)
                .expect("registered");
            ledger.appetite_mb_locked(&state, entry)
        };
        assert_eq!(
            (appetite("g/a"), appetite("g/b")),
            (12_000.0, 12_000.0),
            "the whole card is the ceiling on an appetite, so the conferred \
             anchor weighs no more than the honest one"
        );
    }
    // And the split follows: an even one, where the unclamped 3 072 would
    // have carried 3072/(3072+960) of the headroom.
    b.note_demand(4);
    let token = a.request_grant(u64::MAX, None, 4, 0).unwrap();
    assert_eq!(token.grant().mb, 5_000, "half of the 10 GB headroom");
    drop(token);
    drop(b);
}

/// The store is keyed by **architecture**, so on a machine with two cards of
/// one architecture the small card adopts the big card's anchor as a seeded
/// claim, with the backstop live under it.
#[test]
fn a_second_card_of_the_same_architecture_adopts_the_anchor_as_seeded() {
    const SMALL: &str = "GPU-bbbb";
    let profiles = Arc::new(FakeProfiles {
        // Written by this machine — on its 96 GB card.
        seed: Some(seeded_anchor(512, true)),
        ..FakeProfiles::default()
    });
    let ledger = VramLedger::for_test_with(
        &[(GPU, "TEST 9000", 100_000), (SMALL, "TEST 1000", 12_000)],
        no_margin(),
        Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
    );
    let handle = loaded_on(SMALL, Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 11_000, 0);
    ledger.ingest_all_for_test();
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    assert_eq!(
        ledger
            .calibration_state("g/a", SMALL)
            .unwrap()
            .max_units_measured,
        256,
        "the small card never ran 512, whichever file the number came from"
    );
}

/// A conferred anchor floors the ramp's **exponent**, rounded down; the
/// ratchet ceiling above it is unchanged.
#[test]
fn a_conferred_anchor_never_admits_a_window_wider_than_itself() {
    let seeded = |anchor: u64| {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(anchor, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(1_000_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 900_000, 0);
        ledger.ingest_all_for_test();
        (ledger, handle, admission)
    };
    let (_ledger, handle, admission) = seeded(3072);
    assert_eq!(
        measured_window(&handle, &admission, 2048),
        2048,
        "64 << 5, not 64 << 6: never wider than the anchor itself"
    );
    // And the ceiling above it is unchanged: the clean window earns the
    // ramp its next step, still inside RATCHET_FACTOR x 3072.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 4096);
    drop(token);

    let (_ledger, _handle, admission) = seeded(768);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        512,
        "and leg 2's conferred 768 opens at 512, not 1024"
    );
}

/// A profile measured on a **different SKU of the same architecture** prices
/// this card's windows and floors its ramp, but the budget is still bounded by
/// this card's live free memory.
#[test]
fn a_profile_from_another_sku_of_this_architecture_prices_and_floors_it() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: None,
            // Measured on a 32 GB card of this architecture; this host's
            // card holds 12 GB. Not local: it is not this machine's own.
            local: false,
            fit_is_local: false,
            exact_torch: true,
            max_units_measured: 4096,
            local_samples: 99,
            knee_clean_windows: 0,
            ring: Vec::new(),
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(12_288, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 11_000, 0);
    ledger.ingest_all_for_test();

    let gpu = &ledger.health()[0];
    assert_eq!(
        (gpu.gpu_arch.as_deref(), gpu.total_mb),
        (Some(ARCH), 12_288),
        "one architecture, two capacities: the capacity is read here, not \
         taken from the profile"
    );
    assert_eq!(
        gpu.workers[0].max_units_measured, 4096,
        "the bigger card's anchor is conferred here too"
    );
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert!(
        token.grant().mb <= gpu.headroom_mb,
        "and the smaller card's own headroom is what bounds it: {} vs {}",
        token.grant().mb,
        gpu.headroom_mb
    );
    assert_eq!(
        token.grant().unit_budget,
        876,
        "not 4096: this card's headroom, priced through the borrowed slope"
    );
}

/// The host's own probe seeds the architecture, so a worker naming another
/// one (as `HSA_OVERRIDE_GFX_VERSION` makes torch do) is resolved in the
/// host's favour, with one WARN per card.
#[test]
fn a_worker_naming_another_architecture_is_reported_once_per_card() {
    let ledger = ledger(24_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    handle
        .lock()
        .unwrap()
        .load
        .as_mut()
        .expect("the load report")
        .value
        .gpu_arch = Some("gfx1030".to_owned());

    for model in ["g/a", "g/b"] {
        ledger
            .register_worker(model, item_cost(4), &handle, None)
            .expect("admitted");
    }

    assert_eq!(
        ledger.gpu_arch(GPU).as_deref(),
        Some(ARCH),
        "the host's own seed still wins the key"
    );
    assert_eq!(
        ledger.lock().arch_mismatch_logged.len(),
        1,
        "and the disagreement is reported once for the card, not per replica"
    );
}

/// A **local** profile resumes the measured range: the anchor floors the ramp
/// and the sample ring comes back, so the ramp is not paid again per restart.
#[test]
fn a_local_profile_resumes_the_measured_range() {
    let ring: Vec<FitSample> = (1..=6)
        .map(|k| FitSample {
            units: k * 8,
            delta_mb: 10 * k * 8,
        })
        .collect();
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 6,
            knee_units: None,
            local: true,
            fit_is_local: true,
            exact_torch: true,
            max_units_measured: 64,
            local_samples: 6,
            knee_clean_windows: 0,
            ring: ring.clone(),
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    ledger.ingest_all_for_test();

    let state = ledger.calibration_state("g/a", GPU).expect("seeded");
    assert_eq!(
        state.max_units_measured, 64,
        "the anchor survived the restart"
    );
    assert_eq!(state.samples, ring, "and so did the ring the fit runs on");
    assert_eq!(ledger.health()[0].workers[0].local_samples, 6);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        64,
        "resumes at the measured range instead of re-ramping from the seed"
    );
}

/// A second replica of the same model on the same GPU must not re-seed:
/// what it would overwrite is this run's own measurements.
#[test]
fn seeding_happens_once_per_model_and_gpu() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 6,
            knee_units: None,
            local: true,
            fit_is_local: true,
            exact_torch: true,
            max_units_measured: 64,
            local_samples: 6,
            knee_clean_windows: 0,
            ring: vec![FitSample {
                units: 64,
                delta_mb: 640,
            }],
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let first = loaded(Some(1000), Some(0));
    let _a = ledger
        .register_worker("g/a", item_cost(4), &first, None)
        .unwrap();
    let second = loaded(Some(1000), Some(0));
    let _b = ledger
        .register_worker("g/a", item_cost(4), &second, None)
        .unwrap();
    assert_eq!(
        ledger.calibration_state("g/a", GPU).unwrap().samples.len(),
        1,
        "the ring was restored once, not once per replica"
    );
}

/// The write policy: a settled window persists only when the anchor advanced
/// or the fit meaningfully changed, and never before this machine has
/// measured anything of its own.
#[test]
fn the_write_policy_fires_on_evidence_not_per_window() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);

    // Windows that measure nothing teach nothing, so they persist nothing.
    for _ in 0..5 {
        clean_window(&admission);
    }
    assert!(
        profiles.updates.lock().unwrap().is_empty(),
        "no local evidence yet, so nothing is written"
    );

    // Every measured window advances the anchor, so every one of them is a write.
    for units in [4, 8, 16] {
        measured_window(&handle, &admission, units);
    }
    let written = profiles.updates.lock().unwrap().len();
    assert_eq!(written, 3, "one per anchor advance");
    let last = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(last.inference_id, "g/a");
    assert_eq!(last.arch, ARCH, "keyed by GPU architecture");
    assert_eq!(
        last.gpu_name, "TEST 9000",
        "with the SKU recorded as provenance"
    );
    assert_eq!(last.torch, "2.7.1+cu128");
    assert_eq!(last.dtype, "fp16");
    assert_eq!(last.epoch, 1);
    assert_eq!(last.unit, "item");
    assert_eq!(last.aggregation, "count");
    assert_eq!(last.base_mb, 1000);
    assert_eq!(last.base_method.as_deref(), Some("nvml"));
    assert_eq!(last.max_units_measured, 16);
    assert_eq!(last.local_samples, 3);
    assert_eq!(
        last.ring.len(),
        3,
        "the ring rides along so a restart refits"
    );

    // More clean windows that measure nothing again change nothing.
    for _ in 0..5 {
        clean_window(&admission);
    }
    assert_eq!(
        profiles.updates.lock().unwrap().len(),
        written,
        "a settle with no anchor advance and no fit change writes nothing"
    );

    // A batch smaller than the anchor does not advance it, but a size the
    // ring has not held moves the fit.
    measured_window(&handle, &admission, 12);
    let updates = profiles.updates.lock().unwrap();
    assert_eq!(updates.len(), written + 1, "the refit is a reason to write");
    assert_eq!(updates.last().unwrap().max_units_measured, 16);
    assert_eq!(updates.last().unwrap().local_samples, 4);
}

/// A **local** profile matched through the `major.minor` fallback restores
/// this machine's anchor and ring but not its confirmation: the samples are
/// re-earned under the new torch build, widened until then.
#[test]
fn a_fallback_matched_local_profile_confers_growth_but_not_confirmation() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 6,
            knee_units: None,
            local: true,
            fit_is_local: true,
            // The store fell back across torch builds to find this.
            exact_torch: false,
            max_units_measured: 64,
            local_samples: 6,
            knee_clean_windows: 0,
            ring: vec![FitSample {
                units: 64,
                delta_mb: 740,
            }],
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(100_000, VramBudget::default(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 50_000, 0);
    ledger.ingest_all_for_test();
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.max_units_measured, 64,
        "the anchor is this machine's own measurement whatever torch built it"
    );
    assert_eq!(
        worker.local_samples, 0,
        "but a different torch build confirms nothing"
    );
    assert_eq!(
        worker.effective_margin,
        DEFAULT_MARGIN + UNCONFIRMED_MARGIN_BONUS,
        "so it runs widened until this build has confirmed it"
    );

    // And confirmation is re-earned locally, exactly as on a fresh
    // install with a shipped baseline.
    for units in [64, 128, 256, 512, 1024] {
        measured_window(&handle, &admission, units);
    }
    let worker = &ledger.health()[0].workers[0];
    assert!(worker.local_samples >= LOCAL_CONFIRMATION_SAMPLES);
    assert_eq!(worker.effective_margin, DEFAULT_MARGIN);
}

/// A TTL unload and reload must not re-import the ring this run just wrote:
/// the seed flag is set on the first lookup **attempt**, not the first match.
#[test]
fn a_reload_resumes_a_written_profile_without_duplicating_its_ring() {
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
    push_memory(&handle, 90_000, 0);
    for units in [4, 8, 16] {
        measured_window(&handle, &admission, units);
    }
    let before = ledger.calibration_state("g/a", GPU).expect("measured");
    assert_eq!(before.samples.len(), 3);
    assert_eq!(before.max_units_measured, 16);
    // The store really would answer now — that is the whole hazard.
    assert!(
        store.lookup(&item_query("g/a")).is_some(),
        "this run's own profile is on disk"
    );

    // TTL unload, then the same model loads again on the same GPU.
    drop(admission);
    let handle = loaded(Some(1000), Some(0));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    let after = ledger.calibration_state("g/a", GPU).expect("still there");
    assert_eq!(
        after.samples, before.samples,
        "the persisted ring was not appended onto the live one"
    );
    assert_eq!(
        after.max_units_measured, 16,
        "and the anchor resumes rather than doubling back"
    );
}

/// A seeded fit is never written back stamped with our generator: anchor,
/// ring and sample count are local from the first sample, the fit only once
/// a local refit has produced it.
#[test]
fn a_seeded_fit_is_never_laundered_into_local_provenance() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 3.5,
            residual_mb: 42.0,
            samples: 20,
            knee_units: None,
            // A shipped baseline: pricing, nothing else.
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

    // One local sample: the anchor advanced, so the entry is written —
    // but the fit in force is still the baseline's.
    measured_window(&handle, &admission, 4);
    let first = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(first.max_units_measured, 4);
    assert_eq!(first.local_samples, 1);
    assert!(
        !first.ring.is_empty(),
        "the ring is local evidence and travels"
    );
    assert_eq!(
        (first.slope_mb_per_unit, first.residual_mb, first.samples),
        (0.0, 0.0, 0),
        "no fit fields for a fit this machine did not compute"
    );

    // MIN_FIT_SAMPLES local samples produce a local refit, and that one
    // does travel.
    measured_window(&handle, &admission, 8);
    measured_window(&handle, &admission, 16);
    let last = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert!(
        last.slope_mb_per_unit > 0.0,
        "the local refit's values are written: {last:?}"
    );
    assert_eq!(last.samples, MIN_FIT_SAMPLES);
}

/// A worker the store could not key (no torch build, dtype or measured base)
/// is never persisted: the entry could not be read back.
#[test]
fn an_unkeyable_worker_is_never_persisted() {
    for report in [
        LoadReport {
            base_mb: Some(1000),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_uuid: Some(GPU.to_owned()),
            dtype: Some("fp16".to_owned()),
            ..LoadReport::default()
        },
        LoadReport {
            base_mb: Some(1000),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_uuid: Some(GPU.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            ..LoadReport::default()
        },
        LoadReport {
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_uuid: Some(GPU.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            dtype: Some("fp16".to_owned()),
            ..LoadReport::default()
        },
    ] {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(report));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        measured_window(&handle, &admission, 4);
        assert!(
            profiles.updates.lock().unwrap().is_empty(),
            "an incomplete profile key is never written"
        );
    }
}

/// `"unstated"` is a dtype like any other here.
#[test]
fn an_unstated_dtype_still_keys_and_persists() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1000),
        base_method: Some("nvml".to_owned()),
        reserved_at_load_mb: Some(0),
        allocated_at_load_mb: Some(0),
        gpu_uuid: Some(GPU.to_owned()),
        torch_version: Some("2.7.1+cu128".to_owned()),
        dtype: Some("unstated".to_owned()),
        dtype_method: Some("unstated".to_owned()),
        ..LoadReport::default()
    }));
    let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    measured_window(&handle, &admission, 4);

    let update = profiles
        .updates
        .lock()
        .unwrap()
        .last()
        .cloned()
        .expect("a measured window with a full key is persisted");
    assert_eq!(update.dtype, "unstated", "the sentinel is stored verbatim");
    assert_eq!(
        update.dtype_method.as_deref(),
        Some("unstated"),
        "and the method it came from rides along, additively"
    );
    assert_eq!(update.torch, "2.7.1+cu128");
    assert_eq!(update.base_mb, 1000);
    assert_eq!(update.max_units_measured, 4);
    assert!(
        ledger.lock().profile_skip_logged.is_empty(),
        "and nothing was skipped, so nothing was explained"
    );
}

/// A worker that cannot be keyed says why, once per model, GPU and reason,
/// including a missing architecture on MPS and CPU.
#[test]
fn an_unpersistable_worker_says_why_once() {
    for (report, reason) in [
        (
            LoadReport {
                base_mb: Some(1000),
                base_method: Some("nvml".to_owned()),
                reserved_at_load_mb: Some(0),
                allocated_at_load_mb: Some(0),
                gpu_uuid: Some(GPU.to_owned()),
                dtype: Some("fp16".to_owned()),
                ..LoadReport::default()
            },
            "no_torch",
        ),
        (
            LoadReport {
                base_mb: Some(1000),
                base_method: Some("nvml".to_owned()),
                reserved_at_load_mb: Some(0),
                allocated_at_load_mb: Some(0),
                gpu_uuid: Some(GPU.to_owned()),
                torch_version: Some("2.7.1+cu128".to_owned()),
                ..LoadReport::default()
            },
            "no_dtype",
        ),
        (
            LoadReport {
                reserved_at_load_mb: Some(0),
                allocated_at_load_mb: Some(0),
                gpu_uuid: Some(GPU.to_owned()),
                torch_version: Some("2.7.1+cu128".to_owned()),
                dtype: Some("fp16".to_owned()),
                ..LoadReport::default()
            },
            "no_base",
        ),
        (
            LoadReport {
                base_mb: Some(1000),
                base_method: Some("nvml".to_owned()),
                reserved_at_load_mb: Some(0),
                allocated_at_load_mb: Some(0),
                gpu_uuid: Some(GPU.to_owned()),
                torch_version: Some("2.7.1+cu128".to_owned()),
                dtype: Some("fp16".to_owned()),
                ..LoadReport::default()
            },
            "no_arch",
        ),
    ] {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        if reason == "no_arch" {
            // A host whose own probe cannot name one, as MPS and CPU are.
            ledger.lock().gpus.get_mut(GPU).expect("the GPU").arch = None;
        }
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(report));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // Several settles, because the write policy runs on every one.
        for _ in 0..5 {
            measured_window(&handle, &admission, 4);
        }
        assert!(
            profiles.updates.lock().unwrap().is_empty(),
            "an incomplete profile key is still never written"
        );
        let logged: Vec<(String, String, &'static str)> =
            ledger.lock().profile_skip_logged.iter().cloned().collect();
        assert_eq!(
            logged,
            vec![("g/a".to_owned(), GPU.to_owned(), reason)],
            "one line, naming the model and the missing field"
        );
    }
}

/// The shape the calibration store persists: the ratchet anchor, the fit
/// sample ring and the fit, all serde-able.
#[test]
fn calibration_state_exports_the_persistable_shape() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    assert!(
        ledger.calibration_state("g/a", GPU).is_none(),
        "nothing measured yet"
    );
    let series: Vec<BatchMeasurement> = (1..=6u64)
        .map(|k| measurement(k * 8, 0, 10 * k * 8))
        .collect();
    handle.lock().unwrap().record_measurements(series);
    clean_window(&admission);

    let state = ledger.calibration_state("g/a", GPU).expect("exports");
    assert_eq!(state.inference_id, "g/a");
    assert_eq!(state.gpu, GPU);
    assert_eq!(state.max_units_measured, 48, "the ratchet anchor");
    assert_eq!(state.samples.len(), 6);
    assert_eq!(
        state.samples[0],
        FitSample {
            units: 8,
            delta_mb: 80
        }
    );
    let fit = state.fit.expect("fitted");
    assert!((fit.slope_mb_per_unit - 10.0).abs() < 1e-6);

    // Round-trips through serde, which is the whole point of the seam.
    let json = serde_json::to_string(&state).expect("serializes");
    let back: CalibrationState = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(back, state);
    assert!(
        ledger.calibration_state("g/a", "GPU-elsewhere").is_none(),
        "keyed per GPU"
    );
}
