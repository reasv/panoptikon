use super::*;

/// A replica that runs out of memory on a **memory-blind one-item** window
/// has no room to wait for and nothing smaller to fall back on: after
/// [`OOM_WINDOWS_AT_FLOOR`] such windows the settle declares it
/// unrunnable, naming the base and the card's room, and the dispatcher
/// fails the model instead of the next item (Windows run4, W-A1: 1 124
/// failed items and one out-of-memory apiece).
#[test]
fn oom_at_the_one_item_floor_declares_the_replica_unrunnable() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(9_900), Some(0));
    let admission = ledger
        .register_worker("g/big", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 0, 0);
    ledger.ingest_all_for_test();
    let oom = || WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    };
    // A clean window in between clears the count, so a neighbour's spike
    // cannot walk it up over a whole job.
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        token.grant().unit_budget,
        1,
        "a memory-blind window is one item, never the seed batch"
    );
    assert!(token.finish(oom()).is_none(), "one is not evidence");
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert!(
        token
            .finish(WindowOutcome::Responded { oom: None })
            .is_none()
    );
    let mut verdict = None;
    for window in 0..OOM_WINDOWS_AT_FLOOR {
        assert!(
            verdict.is_none(),
            "not before window {OOM_WINDOWS_AT_FLOOR}"
        );
        let _ = window;
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        verdict = token.finish(oom());
    }
    let verdict = verdict.expect("the replica cannot run this model here");
    assert_eq!(verdict.inference_id, "g/big");
    assert_eq!(verdict.gpu, GPU);
    assert_eq!(verdict.base_mb, 9_900, "the measured base");
    assert_eq!(
        verdict.room_mb, 9_900,
        "the card's limit: all of it but the 100 MB another process holds"
    );
    assert!(
        verdict.to_string().contains("g/big") && verdict.to_string().contains("9900"),
        "the reason carries both numbers: {verdict}"
    );
    // The figure that will refuse the next load is in the sentence too,
    // or the operator cannot connect the two lines — and each of the two
    // rooms says which one it is, they being a MiB apart here.
    assert!(
        verdict
            .to_string()
            .contains("9900 MiB this GPU lends a window after its reserve"),
        "the window's room, named: {verdict}"
    );
    assert!(
        verdict
            .to_string()
            .contains("9901 MiB free before the reserve"),
        "and what it will be refused under: {verdict}"
    );
}

/// The shape run5 T2 measured on the 5090, where the rule that only read
/// `mb == 0` never fired: once the model is resident its 31 150 MiB are
/// *ours*, `external` falls, and the card reports a few hundred MiB of
/// nominal share — which every one-item window still ran out of memory
/// in, 8 002 times. The room against one item's cost is what says the
/// replica is at its floor, not the price of the window.
#[test]
fn a_one_item_oom_with_less_room_than_one_item_costs_condemns() {
    let ledger = ledger(32_607, no_margin());
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
        assert_eq!(
            token.grant().mb,
            305,
            "priced, at a few hundred MiB as T2 was: the old rule looked \
             for a price of nothing and so never fired"
        );
        verdict = token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
    }
    let verdict = verdict.expect("three windows at the floor condemn it");
    assert_eq!(verdict.base_mb, 31_150);
    assert_eq!(
        verdict.needs_mb, 31_607,
        "more room than the window that failed had (31 150 + 306), \
         floored just over the card's 31 606 MiB of reserve-less room"
    );
}

/// The same one-item out-of-memory on a card with **room to spare** is the
/// backstop's ordinary business: it deflates and recovers, and no number
/// of them condemns the replica (`calibfixture/oom_cuda`, which fails
/// every predict on an idle 96 GB card).
#[test]
fn a_one_item_oom_with_room_to_spare_condemns_nothing() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/oomy", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 90_000, 0);
    ledger.ingest_all_for_test();
    for _ in 0..(4 * OOM_WINDOWS_AT_FLOOR) {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(
            token.grant().mb > 0,
            "the GPU has room; the window is priced"
        );
        assert!(
            token
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                })
                .is_none(),
            "deflated, not condemned"
        );
    }
}

/// Both measured collapses, replayed against the rule that has to tell
/// them apart: the Windows sysmem fallback, whose 304 MiB of growth had
/// 297 MiB of card to grow into, and the 3090's heterogeneous-corpus drop,
/// every MiB of whose growth fitted (design doc, "The worker's verdict is
/// a candidate").
#[test]
fn the_two_measured_collapses_are_told_apart() {
    for (label, total_mb, free_mb, before_mb, peak_mb, units, rate, deflation) in [
        (
            "selftest-gpu1-oom",
            32_607u64,
            297u64,
            41_374u64,
            41_678u64,
            8u64,
            0.278,
            1u32,
        ),
        ("run4 F3", 24_576, 20_975, 2_830, 5_762, 116, 13.0, 0),
    ] {
        let ledger = ledger(total_mb, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, total_mb / 2, 0);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                free_mb: Some(free_mb),
                free_source: Some("nvml".to_owned()),
                reserved_before_mb: Some(before_mb),
                peak_reserved_mb: Some(peak_mb),
                ..warm_batch(units, rate)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].deflation,
            deflation,
            "{label}: {before_mb} -> {peak_mb} MiB of pool against \
             {free_mb} MiB free"
        );
    }
}

/// An uncorroborated collapse is discarded **whole**: it deflates nothing,
/// and it teaches nothing either — a size the worker called a spill must
/// not become the measured-clean floor the ramp resumes at, nor a row the
/// next process starts from.
#[test]
fn an_uncorroborated_collapse_is_discarded_whole() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    for expected in [4, 8, 16, 32] {
        assert_eq!(measured_window(&handle, &admission, expected), expected);
    }
    let before = anchors(&ledger, "g/a", GPU);
    let samples_before = fit_sample_count(&ledger);
    let stored_before = stored_anchor(&profiles);

    let logs = captured_logs(|| {
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 64);
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                ..measurement(64, 0, 5_000)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
    });

    assert_eq!(
        ledger.health()[0].workers[0].deflation,
        0,
        "5 000 MiB of growth with 90 000 free spilled nothing"
    );
    assert_eq!(anchors(&ledger, "g/a", GPU), before, "not a clean size");
    assert_eq!(fit_sample_count(&ledger), samples_before, "not a fit point");
    assert_eq!(stored_anchor(&profiles), stored_before, "and not persisted");
    assert_eq!(
        logs.iter()
            .filter(|(level, message)| *level == tracing::Level::DEBUG
                && message.contains("grew by less than the device had free"))
            .count(),
        1,
        "said once for the window, at debug"
    );
}

/// The rule reads this batch's figures and nothing else: a replica that
/// reported no load footprint at all still has its collapse judged on the
/// growth, so a 20 GB model on a card with 2 GB free does not deflate on a
/// batch that over-committed nothing.
#[test]
fn a_collapse_is_judged_on_the_batch_not_on_the_load_report() {
    let ledger = ledger(24_576, no_margin());
    let handle = loaded(None, Some(20_000));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 2_000, 20_000);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            throughput_collapse: true,
            free_mb: Some(2_000),
            free_source: Some("nvml".to_owned()),
            reserved_before_mb: Some(20_000),
            peak_reserved_mb: Some(20_100),
            ..warm_batch(64, 1.0)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].workers[0].deflation, 0);
}

/// The peak is the evidence, not the pool the batch ended on: an allocator
/// that released its blocks mid-batch to retry — which is what a card
/// under real pressure does — reports a small after-figure, and reading
/// that one would miss exactly the population the rule is for.
#[test]
fn a_spill_the_allocator_released_mid_batch_still_deflates() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 5_000, 3_000);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            throughput_collapse: true,
            free_mb: Some(5_000),
            free_source: Some("nvml".to_owned()),
            reserved_before_mb: Some(3_000),
            peak_reserved_mb: Some(200_000),
            reserved_after_mb: Some(3_000),
            ..warm_batch(64, 1.0)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
}

/// A RAM-priced host is judged on the same two figures in its own
/// currency — available RAM against the RSS pool's growth — so the load
/// report's basis, where `base_mb` is the load window's RSS *growth* and
/// `reserved_at_load_mb` the absolute high-water, cannot under-state the
/// bar: a batch that grew 100 MiB with 500 MiB available does not deflate.
#[test]
fn a_ram_priced_collapse_is_judged_on_the_same_growth() {
    let ledger = cpu_ledger(no_margin());
    let handle = loaded_cpu(Some(CPU_RAM_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, CPU_RAM_MB / 2, 3_000);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            throughput_collapse: true,
            free_mb: Some(500),
            free_source: Some("rss".to_owned()),
            reserved_before_mb: Some(3_000),
            peak_reserved_mb: Some(3_100),
            ..warm_batch(64, 1.0)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].workers[0].deflation, 0);
}

/// MPS is covered, and in the RAM domain: a Metal allocation spends
/// unified memory, so the room a pool grows into is what the machine has
/// available — the same domain [`VramLedger::external_locked`] sums the
/// rest of the machine in, and not `recommended_max_memory()`.
#[test]
fn a_collapse_on_a_unified_device_is_judged_in_the_ram_domain() {
    const TOTAL: u64 = 110_100;
    const BASE: u64 = 1_000;
    for (label, hog, pool, peak_mb, deflation) in [
        // 35 072 MiB of RAM is left under a 70 000 MiB hog: 15 500 MiB of
        // growth fits in it and 36 000 MiB does not.
        ("inside the room", 70_000u64, 25_000u64, 40_500u64, 0u32),
        ("past the room", 70_000, 25_000, 61_000, 1),
        // And the leg the domain decides: on an idle machine the free
        // reading is clipped to `recommended_max`, 14 972 MiB below the
        // RAM this growth really had.
        ("past the clipped reading only", 0, 5_000, 120_000, 0),
    ] {
        let available = MAC_RAM_MB - hog - BASE - pool;
        let mps = mps_ledger();
        let handle = loaded_mps(Some(TOTAL));
        let admission = mps
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_ram(&handle, TOTAL, available, pool, 12_000);
        clean_window(&admission);
        assert_eq!(mps.health()[0].external_mb, hog, "{label}");
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                reserved_before_mb: Some(pool),
                peak_reserved_mb: Some(peak_mb),
                ..warm_batch(4, 1.0)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            mps.health()[0].workers[0].deflation,
            deflation,
            "{label}: {pool} -> {peak_mb} MiB of pool with {available} MiB \
             of RAM available"
        );
    }
}

/// P5-5: a throughput collapse reported from a window a neighbour was running
/// through is not a negative sample.
#[test]
fn a_collapse_only_deflates_when_the_replica_had_the_gpu_to_itself() {
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

    let held = neighbour.request_grant(4, None, 1, 0).unwrap();
    let token = admission.request_grant(8, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![spilled_past_free(8, 10.0, 90_000)]);
    token.finish(WindowOutcome::Responded { oom: None });
    held.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0]
            .workers
            .iter()
            .find(|worker| worker.inference_id == "g/a")
            .expect("registered")
            .deflation,
        0,
        "a neighbour's window explains the rate drop"
    );

    // Alone, the identical measurement is the WDDM spill signal the flag
    // was added for.
    let token = admission.request_grant(8, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![spilled_past_free(8, 10.0, 90_000)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0]
            .workers
            .iter()
            .find(|worker| worker.inference_id == "g/a")
            .expect("registered")
            .deflation,
        1
    );
}

/// Suppressing the collapse verdict must not suppress the **OOM** riding on the
/// same measurement.
#[test]
fn a_suppressed_collapse_still_reports_the_oom_it_rode_in_with() {
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

    let held = neighbour.request_grant(4, None, 1, 0).unwrap();
    let token = admission.request_grant(8, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            ..spilled_past_free(8, 10.0, 90_000)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    held.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0]
            .workers
            .iter()
            .find(|worker| worker.inference_id == "g/a")
            .expect("registered")
            .deflation,
        1,
        "the neighbour explains the rate drop; it does not explain the \
         allocator giving up"
    );
}

/// R3's host half, the tier that needs no corroboration: a typed exception is the
/// interpreter naming the condition, and it deflates whatever the GPU's free
/// reading says — a caching allocator can fail with gigabytes free and fragmented.
#[test]
fn a_typed_out_of_memory_class_deflates_without_corroboration() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let granted_mb = token.grant().mb;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            oom_class: Some(OomClass {
                source: OOM_SOURCE_TYPED.to_owned(),
                exception: "torch.OutOfMemoryError".to_owned(),
                free_mb_at_failure: Some(granted_mb * 10),
                device: "cuda:0".to_owned(),
            }),
            ..measurement(4, 0, 900)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
}

/// R3's host half, the tier that does: a classification read out of the failure's
/// *wording*, against a GPU whose own live reading at that instant still held the
/// whole envelope this window was priced at.
#[test]
fn a_message_pattern_class_deflates_only_when_the_gpu_was_tight() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let granted_mb = token.grant().mb;
    assert!(granted_mb > 0, "the window has an envelope to be judged on");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            oom_class: Some(OomClass {
                source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
                exception: "RuntimeError".to_owned(),
                free_mb_at_failure: Some(granted_mb.saturating_mul(20)),
                device: "cuda:0".to_owned(),
            }),
            ..measurement(4, 0, 900)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].workers[0].deflation,
        0,
        "the GPU had twenty times this window's envelope free; a batch \
         this size is not what it ran out of"
    );

    // The identical classification, with the GPU actually short of what
    // the window was promised.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let granted_mb = token.grant().mb;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            oom_class: Some(OomClass {
                source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
                exception: "RuntimeError".to_owned(),
                free_mb_at_failure: Some(granted_mb / 2),
                device: "cuda:0".to_owned(),
            }),
            ..measurement(4, 0, 900)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
}

/// A worker that states no class at all is a **pre-run2** one, and its bare `oom`
/// is the contract it was built against.
#[test]
fn a_measurement_with_no_class_is_trusted_as_it_always_was() {
    let honest = BatchMeasurement {
        oom: true,
        ..BatchMeasurement::default()
    };
    let charge = GrantCharge {
        mb: 4_000,
        room: 4000,
        requests: 1,
        unit_budget: 8,
        squeezed: false,
        peak_occupants: 0,
        knee_bound: false,
        ample_headroom: true,
        queue_bound: false,
        byte_bound: false,
    };
    assert_eq!(
        oom_verdict(&honest, Some(&charge)),
        OomVerdict::Trusted(OomTrust::Outright),
        "no class stated"
    );
    for source in [OOM_SOURCE_TYPED, OOM_SOURCE_MARKER] {
        assert_eq!(
            oom_verdict(
                &BatchMeasurement {
                    oom_class: Some(OomClass {
                        source: source.to_owned(),
                        exception: "torch.OutOfMemoryError".to_owned(),
                        free_mb_at_failure: Some(90_000),
                        device: "cuda:0".to_owned(),
                    }),
                    ..honest.clone()
                },
                Some(&charge)
            ),
            OomVerdict::Trusted(OomTrust::Outright),
            "{source} is structural; the free reading has no veto over it"
        );
    }
    assert_eq!(
        oom_verdict(
            &BatchMeasurement {
                oom_class: Some(OomClass {
                    source: "some_future_tier".to_owned(),
                    exception: "X".to_owned(),
                    free_mb_at_failure: Some(90_000),
                    device: "cuda:0".to_owned(),
                }),
                ..honest.clone()
            },
            Some(&charge)
        ),
        OomVerdict::Trusted(OomTrust::Outright),
        "an unrecognised tier is believed, not second-guessed"
    );
    let pattern = BatchMeasurement {
        oom_class: Some(OomClass {
            source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
            exception: "RuntimeError".to_owned(),
            free_mb_at_failure: None,
            device: "cuda:0".to_owned(),
        }),
        ..honest.clone()
    };
    assert_eq!(
        oom_verdict(&pattern, Some(&charge)),
        OomVerdict::Trusted(OomTrust::Unopposed),
        "no reading to contradict it: a veto that cannot fire lets the \
         classification stand — and the log says it stood unopposed"
    );
    assert_eq!(
        oom_verdict(&pattern, Some(&GrantCharge { mb: 0, ..charge })),
        OomVerdict::Trusted(OomTrust::Unopposed),
        "a memory-blind grant states no envelope either"
    );
    assert_eq!(
        oom_verdict(
            &BatchMeasurement {
                oom: false,
                ..honest
            },
            Some(&charge)
        ),
        OomVerdict::None
    );
}

/// MPS pass **F3** (`instruments/mps-selftest-oom-wm005.json`): the MPS
/// allocator refused 1 GiB at its own 5.38 GiB ceiling while the Mac had
/// 103 918 MiB of its 110 100 free. Reported as free RAM that reading
/// contradicts any grant the host could have made and the ledger never
/// deflates; reported as the allocator's headroom — 5 505 MiB of ceiling
/// less the 4 911 it held — the same one rule corroborates it.
#[test]
fn an_mps_ceiling_failure_is_not_vetoed_by_the_ram_beside_it() {
    let charge = GrantCharge {
        mb: 14_430,
        room: 14430,
        requests: 1,
        unit_budget: 512,
        squeezed: false,
        peak_occupants: 0,
        knee_bound: false,
        ample_headroom: true,
        queue_bound: false,
        byte_bound: false,
    };
    let refused = |free_mb_at_failure: u64| BatchMeasurement {
        oom: true,
        oom_class: Some(OomClass {
            source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
            exception: "RuntimeError".to_owned(),
            free_mb_at_failure: Some(free_mb_at_failure),
            device: "mps".to_owned(),
        }),
        ..BatchMeasurement::default()
    };
    assert_eq!(
        oom_verdict(&refused(103_918), Some(&charge)),
        OomVerdict::Contradicted {
            free_mb: 103_918,
            grant_mb: 14_430
        },
        "the RAM beside the allocator is not what refused the batch"
    );
    assert_eq!(
        oom_verdict(&refused(594), Some(&charge)),
        OomVerdict::Trusted(OomTrust::Corroborated),
        "what the allocator had left agrees the batch was too big"
    );
}

/// Run2 defect **C2**.
#[test]
fn an_out_of_memory_negative_names_the_tier_that_classified_it() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let granted_mb = token.grant().mb;
    assert!(granted_mb > 0, "the window has an envelope to be named");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            oom_class: Some(OomClass {
                source: OOM_SOURCE_TYPED.to_owned(),
                exception: "torch.OutOfMemoryError".to_owned(),
                free_mb_at_failure: Some(512),
                device: "cuda:0".to_owned(),
            }),
            ..measurement(4, 0, 900)
        }]);
    let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
    let window = settled.window.expect("the window settled");
    assert_eq!(window.negative_reason, Some("oom"));
    let oom = settled.oom.expect("and the tier line rides with it");
    assert_eq!(oom.inference_id, "g/a");
    assert_eq!(oom.gpu, window.gpu);
    assert_eq!(oom.source, OOM_SOURCE_TYPED);
    assert_eq!(oom.exception, "torch.OutOfMemoryError");
    assert_eq!(
        oom.trust, "trusted",
        "the interpreter named the condition; there is nothing to \
         corroborate"
    );
    assert_eq!(oom.free_mb_at_failure, 512);
    assert_eq!(
        oom.grant_mb, granted_mb,
        "the envelope the veto weighs a reading against, and what \
         deflation acts on"
    );
    assert_eq!(oom.oom_samples, 1);
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
}

/// The tier that *can* be corroborated says whether it was.
#[test]
fn a_message_pattern_negative_says_whether_the_gpu_corroborated_it() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    let pattern = |free_mb_at_failure: Option<u64>| BatchMeasurement {
        oom: true,
        oom_class: Some(OomClass {
            source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
            exception: "RuntimeError".to_owned(),
            free_mb_at_failure,
            device: "cuda:0".to_owned(),
        }),
        ..measurement(4, 0, 900)
    };

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let granted_mb = token.grant().mb;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![pattern(Some(granted_mb / 2))]);
    let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
    let oom = settled.oom.expect("a negative, and an explained one");
    assert_eq!(oom.source, OOM_SOURCE_MESSAGE_PATTERN);
    assert_eq!(
        oom.trust, "corroborated",
        "the worker's own reading at the failure was below the envelope"
    );
    assert_eq!(oom.free_mb_at_failure, (granted_mb / 2) as i64);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![pattern(None)]);
    let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
    let oom = settled.oom.expect("believed, so still a negative");
    assert_eq!(
        oom.trust, "unopposed",
        "a veto that cannot fire is not the same as evidence for"
    );
    assert_eq!(
        oom.free_mb_at_failure, -1,
        "the sentinel for a classification that carried no reading"
    );

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let granted_mb = token.grant().mb;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![pattern(Some(granted_mb.saturating_mul(20)))]);
    let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
    assert_eq!(
        settled.window.expect("settled").negative_reason,
        None,
        "B11's shape: the reading contradicts the wording"
    );
    assert!(
        settled.oom.is_none(),
        "and a window that is not a negative has no tier to name; the \
         veto's own WARN is what speaks there"
    );
}

/// The error-frame path — a `predict` that failed with no measurement to classify —
/// is the host's own reading, and the line credits the host rather than inventing a
/// worker classification.
#[test]
fn an_error_frame_negative_credits_the_tier_that_read_the_frame() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let granted_mb = token.grant().mb;
    let settled = token.finish_for_test(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    assert_eq!(
        settled.window.expect("settled").negative_reason,
        Some("oom")
    );
    let oom = settled.oom.expect("the frame is what classified it");
    assert_eq!(oom.source, OOM_SOURCE_ERROR_FRAME);
    assert_eq!(
        oom.exception, "unknown",
        "an error frame carries no exception type"
    );
    assert_eq!(oom.trust, "trusted");
    assert_eq!(oom.free_mb_at_failure, -1);
    assert_eq!(oom.grant_mb, granted_mb);
    assert_eq!(
        oom.oom_samples, 0,
        "no measurement survived to carry a class"
    );

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let settled = token.finish_for_test(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Marker),
    });
    assert_eq!(
        settled.oom.expect("still a negative").source,
        OOM_SOURCE_MARKER,
        "our own sentinel is not the host recognising prose"
    );
}

/// A pre-run2 worker's bare `oom` flag deflates as it always did, and the
/// log says the tier is missing rather than guessing one — which is how an
/// operator sees that the worker on the other end is an old one.
#[test]
fn a_negative_from_a_worker_that_states_no_tier_says_so() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            oom_class: None,
            ..measurement(4, 0, 900)
        }]);
    let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
    let oom = settled.oom.expect("trusted, as the old contract says");
    assert_eq!(oom.source, OOM_SOURCE_UNCLASSIFIED);
    assert_eq!(oom.exception, "unknown");
    assert_eq!(oom.trust, "trusted");
    assert_eq!(oom.oom_samples, 1);
}

/// A worker that sends the `oom_class` map with its two required strings left
/// empty.
#[test]
fn a_tier_stated_as_an_empty_string_still_names_something() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 1000);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            oom_class: Some(OomClass {
                source: String::new(),
                exception: String::new(),
                free_mb_at_failure: None,
                device: String::new(),
            }),
            ..measurement(4, 0, 900)
        }]);
    let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
    let oom = settled.oom.expect("an unrecognised tier is still believed");
    assert_eq!(
        oom.source, OOM_SOURCE_UNCLASSIFIED,
        "never the empty string"
    );
    assert_eq!(oom.exception, "unknown");
    assert_eq!(
        oom.trust, "trusted",
        "an unrecognised tier is trusted, and the empty one is one of those"
    );
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
}

#[test]
fn oom_messages_are_classified() {
    assert!(message_reports_oom(
        "worker error: INFERENCE_OOM_BATCH_SIZE_1: out of GPU memory"
    ));
    assert!(message_reports_oom(
        "INFERENCE_OOM_WINDOW: batch of 32 failed"
    ));
    assert!(message_reports_oom("CUDA out of memory. Tried to allocate"));
    // The unified backends, whose only negative signal this is: MPS capitalises
    // differently and CPU torch never says "out of memory" at all.
    assert!(message_reports_oom(
        "RuntimeError: MPS backend out of memory (MPS allocated: 96.00 GB)"
    ));
    assert!(message_reports_oom(
        "RuntimeError: [enforce fail at alloc_cpu.cpp:117] . DefaultCPUAllocator: \
         can't allocate memory: you tried to allocate 8589934592 bytes"
    ));
    assert!(!message_reports_oom("ValueError: bad input"));
    // Neither half of the CPU pair means anything on its own, and the
    // pair is per **line**: two halves in unrelated lines of a multi-line
    // blob are two unrelated lines, not an allocator failure.
    assert!(!message_reports_oom(
        "DefaultCPUAllocator: this is some other complaint"
    ));
    assert!(!message_reports_oom(
        "DefaultCPUAllocator: reset\nfailed to allocate memory for the log buffer"
    ));
}

/// The host half of the classifier on the **error-frame** path.
#[test]
fn out_of_memory_needs_a_device_to_be_a_device_out_of_memory() {
    // B11's exact shape, from run1's `failbatch_oomtext` leg: an impl wording an
    // unrelated failure with the words.
    assert!(!message_reports_oom(
        "RuntimeError: refusing merged batch of 32: the caption cache is \
         out of memory slots"
    ));
    // Every one of these is a real wording from a shipped dependency, and
    // every one of them was lost by a closed spelling list.
    for message in [
        "torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 2.00 GiB",
        "RuntimeError: CUDA error: out of memory",
        "RuntimeError: CUDA driver error: out of memory",
        "RuntimeError: cuda runtime error (2) : out of memory",
        "RuntimeError: CUDA failed with error out of memory",
        "RuntimeError: HIP out of memory. Tried to allocate 2.00 GiB",
    ] {
        assert!(message_reports_oom(message), "{message}");
    }
    // The token is a whole word, so the words plus a coincidence are still nothing.
    for message in [
        "RuntimeError: the relationship cache is out of memory slots",
        "RuntimeError: the chip's queue is out of memory slots",
        "RuntimeError: hipster mode ran out of memory slots",
    ] {
        assert!(!message_reports_oom(message), "{message}");
    }
    // And it is per line, which on this path matters more than for the
    // CPU pair: a Python traceback names `torch/cuda/__init__.py` in its
    // frames, and `/` is a word boundary.
    assert!(!message_reports_oom(
        "Traceback (most recent call last):\n  File \
         \"/venv/lib/python3.12/site-packages/torch/cuda/__init__.py\", line 1, in x\n\
         RuntimeError: the caption cache is out of memory slots"
    ));
    // The allocator spellings that never say the words at all are still
    // matched, driver vocabulary included.
    for message in [
        "RuntimeError: CUBLAS_STATUS_ALLOC_FAILED when calling cublasCreate",
        "RuntimeError: cusolver_status_alloc_failed",
        "RuntimeError: hipErrorOutOfMemory",
    ] {
        assert!(message_reports_oom(message), "{message}");
    }
}
