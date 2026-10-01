//! Out-of-memory handling: condemnation at the floor, collapses, and OOM tiers.
use super::*;

/// A replica that runs out of memory on a **memory-blind one-item** window
/// has nothing smaller to fall back on: after [`OOM_WINDOWS_AT_FLOOR`] such
/// windows the settle declares it unrunnable, naming the base and the card's
/// room, and the dispatcher fails the model instead of every item.
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
    // The figure that will refuse the next load is in the sentence too, and
    // each of the two rooms says which one it is.
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

/// A CPU replica of `g/a` with RAM to spare, as a load after a death brings
/// up: the ledger's calibration for the model is the one the last left.
fn cpu_replica(ledger: &Arc<VramLedger>) -> (TelemetryHandle, Admission) {
    let handle = loaded_cpu(Some(CPU_RAM_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory_with_total(&handle, 40_000, 0, Some(CPU_RAM_MB), "ram");
    (handle, admission)
}

fn death_cap(ledger: &Arc<VramLedger>) -> Option<u64> {
    ledger.health()[0].workers[0].death_cap_units
}

/// A worker that dies whenever its batch reaches 32 units dies once: the
/// model is then held at half that batch, where without the cap the next
/// replica ramps back to 32 and dies again, window after window.
#[test]
fn a_death_mid_window_caps_the_model_at_half_the_batch_that_died() {
    const FATAL: u64 = 32;
    let ledger = cpu_ledger(no_margin());
    let mut replica = cpu_replica(&ledger);
    let (mut deaths, mut sizes) = (0, Vec::new());
    for _ in 0..40 {
        let (handle, admission) = &replica;
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget;
        sizes.push(units);
        if units < FATAL {
            handle.lock().unwrap().record_measurements(vec![measurement(
                units,
                0,
                10 * units + 100,
            )]);
            token.finish(WindowOutcome::Responded { oom: None });
            continue;
        }
        deaths += 1;
        token.finish(WindowOutcome::WorkerDied);
        assert_eq!(death_cap(&ledger), Some(FATAL / 2));
        // The model is reloaded: a new replica, the same calibration.
        replica = cpu_replica(&ledger);
    }
    assert_eq!(deaths, 1, "{sizes:?}");
    assert_eq!(sizes[..6], [4, 8, 16, 32, 8, 16], "{sizes:?}");
    assert!(sizes[6..].iter().all(|units| *units == 16), "{sizes:?}");
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 16);

    // A shape ceiling under the cap binds in its place: the smaller rules.
    let (handle, admission) = &replica;
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            clamped: Some(ClampReport {
                from_units: 16,
                to_units: 4,
                free_mb: None,
                reason: Some(CLAMP_REASON_INDEX_LIMIT.to_owned()),
            }),
            ..measurement(4, 0, 140)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        (worker.shape_ceiling_units, worker.unit_budget),
        (Some(4), 4)
    );
    assert_eq!(worker.death_cap_units, Some(16));
}

/// Each further death halves the cap, down to one unit and no further. A
/// window that ends any other way leaves it alone.
#[test]
fn repeated_deaths_halve_the_cap_down_to_one_unit() {
    let ledger = cpu_ledger(no_margin());
    let (handle, admission) = cpu_replica(&ledger);
    for units in [4, 8, 16, 32] {
        measured_window(&handle, &admission, units);
    }
    assert_eq!(death_cap(&ledger), None);
    for outcome in [
        WindowOutcome::Aborted,
        WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        },
    ] {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(outcome);
        assert_eq!(death_cap(&ledger), None, "{outcome:?}");
    }
    drop(admission);

    let mut caps = Vec::new();
    for _ in 0..7 {
        let (_handle, admission) = cpu_replica(&ledger);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget;
        if let Some(cap) = caps.last() {
            assert!(units <= *cap, "{units} units under a cap of {cap}");
        }
        token.finish(WindowOutcome::WorkerDied);
        let cap = death_cap(&ledger).expect("capped");
        assert_eq!(cap, (units / 2).max(1));
        caps.push(cap);
    }
    assert_eq!(caps, [16, 8, 4, 2, 1, 1, 1]);
}

/// A window the queue sized says nothing about the batch the model can run:
/// its death sets no cap, whether the unit is an item or a pixel. A full
/// window that dies still caps at half.
#[test]
fn a_death_in_a_window_the_queue_sized_sets_no_cap() {
    let megapixel = CostDimension {
        unit: CostUnit::Pixel,
        aggregation: Some(CostAggregation::Sum),
        seed_units: Some(8_000_000),
        ..item_cost(4)
    };
    // (cost, units per MiB, the units of the one request in the short window)
    for (cost, per_mb, short) in [
        (item_cost(4), 1, 1),
        (item_cost(4), 1, 3),
        (megapixel, 1_000_000, 1_000_000),
    ] {
        let ledger = cpu_ledger(no_margin());
        let replica = || {
            let handle = loaded_cpu(Some(CPU_RAM_MB));
            let admission = ledger
                .register_worker("g/a", cost, &handle, None)
                .expect("admitted");
            push_memory_with_total(&handle, 40_000, 0, Some(CPU_RAM_MB), "ram");
            (handle, admission)
        };
        let (handle, admission) = replica();
        let mut full = 0;
        for _ in 0..7 {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            full = token.grant().unit_budget;
            let batch = measurement(full, 0, 100 + full / per_mb);
            handle.lock().unwrap().record_measurements(vec![batch]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        assert_eq!(full, cost.seed_units.map_or(0, u64::from) << 6);

        let token = admission.request_grant(short, None, 1, 0).expect("granted");
        assert_eq!(token.grant().unit_budget, short, "the request's own size");
        token.finish(WindowOutcome::WorkerDied);
        assert_eq!(death_cap(&ledger), None, "{short} units");
        drop(admission);
        if per_mb > 1 {
            continue;
        }

        // A short window that memory cut further was sized by memory: with
        // 50 MiB free above the reserve, 100 units in hand run as 50.
        let (handle, admission) = replica();
        push_memory_with_total(&handle, CPU_RAM_MB / 10 + 50, 0, Some(CPU_RAM_MB), "ram");
        let token = admission.request_grant(100, None, 1, 0).expect("granted");
        assert!(token.grant().squeezed);
        assert_eq!(token.grant().unit_budget, 50);
        token.finish(WindowOutcome::WorkerDied);
        assert_eq!(death_cap(&ledger), Some(25));
        ledger.lock().calibration.values_mut().for_each(|cal| {
            cal.death_cap_units = None;
        });
        drop(admission);

        // The reloaded model is back near its full size (each death halved
        // the anchor), and a death in a full window caps.
        let (_handle, admission) = replica();
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget;
        assert!(units >= full / 4, "{units} after {full}");
        token.finish(WindowOutcome::WorkerDied);
        assert_eq!(death_cap(&ledger), Some(units / 2));
    }
}

/// A worker that dies at one unit cannot be given a smaller batch. Three
/// such deaths in a row, each a fresh replica, condemn the model as three
/// one-item out-of-memory windows do, whatever room the ledger saw; a clean
/// window in between starts the count again.
#[test]
fn three_deaths_in_a_row_at_one_unit_condemn_the_model() {
    let cpu = cpu_ledger(no_margin());
    let one_unit_window = |outcome| {
        let handle = loaded_cpu(Some(CPU_RAM_MB));
        let admission = cpu
            .register_worker("g/a", item_cost(1), &handle, None)
            .expect("admitted");
        push_memory_with_total(&handle, 40_000, 0, Some(CPU_RAM_MB), "ram");
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, 1);
        assert!(token.grant().mb > 0, "with room to spare");
        token.finish(outcome)
    };
    let clean = WindowOutcome::Responded { oom: None };
    // An out-of-memory error with room to spare is no strike, as before.
    {
        let handle = loaded_cpu(Some(CPU_RAM_MB));
        let admission = cpu
            .register_worker("g/a", item_cost(1), &handle, None)
            .expect("admitted");
        push_memory_with_total(&handle, 40_000, 0, Some(CPU_RAM_MB), "ram");
        for _ in 0..OOM_WINDOWS_AT_FLOOR {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let oom = Some(ErrorFrameOom::Prose);
            assert!(token.finish(WindowOutcome::Responded { oom }).is_none());
        }
    }
    assert!(one_unit_window(WindowOutcome::WorkerDied).is_none());
    assert!(one_unit_window(WindowOutcome::WorkerDied).is_none());
    assert!(one_unit_window(clean).is_none());
    assert!(!cpu.was_condemned("g/a", "CPU"));
    for _ in 1..OOM_WINDOWS_AT_FLOOR {
        assert!(one_unit_window(WindowOutcome::WorkerDied).is_none());
    }
    // An abort neither counts nor clears.
    assert!(one_unit_window(WindowOutcome::Aborted).is_none());
    let verdict = one_unit_window(WindowOutcome::WorkerDied).expect("the third in a row");
    assert_eq!(
        (verdict.inference_id.as_str(), verdict.gpu.as_str()),
        ("g/a", "CPU")
    );
    assert!(cpu.was_condemned("g/a", "CPU"));

    // On private memory with no host RAM booked a death teaches nothing.
    let ledger = ledger(100_000, no_margin());
    for _ in 0..(2 * OOM_WINDOWS_AT_FLOOR) {
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(1), &handle, None)
            .expect("admitted");
        push_memory(&handle, 60_000, 0);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(token.finish(WindowOutcome::WorkerDied).is_none());
    }
    assert!(!ledger.was_condemned("g/a", GPU));
}

/// A fresh CPU replica of `g/a` with RAM to spare whose one window of
/// `units` (its seed) ends in `outcome`.
fn seed_window(
    ledger: &Arc<VramLedger>,
    units: u32,
    outcome: WindowOutcome,
) -> Option<UnrunnableReplica> {
    let handle = loaded_cpu(Some(CPU_RAM_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(units), &handle, None)
        .expect("admitted");
    push_memory_with_total(&handle, 40_000, 0, Some(CPU_RAM_MB), "ram");
    admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted")
        .finish(outcome)
}

/// A verdict reached by deaths says the worker died, with no memory figure,
/// and refuses loads for [`DEATH_VERDICT_LAPSE`] only. The strike count
/// outlives it: the next death at one unit refuses the model again at once,
/// and a clean window clears everything.
#[tokio::test]
async fn a_verdict_reached_by_deaths_says_so_and_lapses() {
    let mut ledger = cpu_ledger(no_margin());
    Arc::get_mut(&mut ledger).unwrap().probe_external = false;
    let died = || seed_window(&ledger, 1, WindowOutcome::WorkerDied);
    let load = || ledger.reserve_load("g/a", item_cost(1), "CPU", None);
    assert!(died().is_none() && died().is_none());
    let verdict = died().expect("the third death in a row");
    assert!(verdict.died);
    assert_eq!(verdict.needs_mb, 0);

    let refusal = load().await.err().expect("refused");
    assert!(refusal.died);
    assert_eq!(refusal.needs_mb, 0);
    for text in [verdict.to_string(), refusal.to_string()] {
        assert!(
            text.contains("died 3 times in a row running a single item on GPU CPU"),
            "{text}"
        );
        assert!(!text.contains("MiB") && !text.contains("memory"), "{text}");
    }

    ledger.age_death_verdicts_for_test(DEATH_VERDICT_LAPSE - Duration::from_secs(1));
    assert!(load().await.is_err(), "one second short");
    ledger.age_death_verdicts_for_test(Duration::from_secs(1));
    assert!(load().await.is_ok(), "one attempt");
    assert!(!ledger.was_condemned("g/a", "CPU"));

    // Still at three strikes: one more death refuses it for another term.
    assert!(died().is_some_and(|verdict| verdict.died));
    assert!(load().await.is_err());

    // A clean window clears the verdict and the count.
    assert!(seed_window(&ledger, 1, WindowOutcome::Responded { oom: None }).is_none());
    assert!(!ledger.was_condemned("g/a", "CPU"));
    assert!(load().await.is_ok());
    assert!(died().is_none() && died().is_none());
    assert!(load().await.is_ok());
}

/// Only a death at one unit is a strike, and only a death passes the count
/// to the next replica: a replica that struck out-of-memory errors and was
/// then replaced leaves none behind.
#[test]
fn a_death_at_two_units_and_a_replaced_replicas_errors_leave_no_strike() {
    let died = WindowOutcome::WorkerDied;
    let cpu = cpu_ledger(no_margin());
    assert!(seed_window(&cpu, 2, died).is_none(), "two units: no strike");
    let key = ("g/a".to_owned(), "CPU".to_owned());
    assert_eq!(cpu.lock().calibration[&key].death_cap_units, Some(1));
    assert!(seed_window(&cpu, 2, died).is_none(), "the first strike");
    assert!(seed_window(&cpu, 2, died).is_none(), "the second");
    assert!(seed_window(&cpu, 2, died).is_some(), "the third");

    // No room for one item: two out-of-memory strikes on one replica.
    let ledger = ledger(10_000, no_margin());
    let strikes = |count| {
        let handle = loaded(Some(9_900), Some(0));
        let admission = ledger
            .register_worker("g/big", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        (0..count).fold(None, |_, _| {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted")
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                })
        })
    };
    assert!(strikes(OOM_WINDOWS_AT_FLOOR - 1).is_none());
    assert!(
        strikes(OOM_WINDOWS_AT_FLOOR - 1).is_none(),
        "its replacement starts from none"
    );
    assert!(strikes(OOM_WINDOWS_AT_FLOOR).is_some());
}

/// The cap does not stand in for the verdict on a model that cannot run one
/// item: a death and two out-of-memory windows at one item, with no room for
/// one, still condemn the replica.
#[test]
fn a_capped_model_is_still_condemned_at_the_one_item_floor() {
    let ledger = cpu_ledger(no_margin());
    let handle = loaded_cpu(Some(CPU_RAM_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory_with_total(&handle, 0, 0, Some(CPU_RAM_MB), "ram");
    ledger.ingest_all_for_test();
    let window = |outcome| {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, 1, "no room: one item");
        token.finish(outcome)
    };
    let oom = WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    };
    assert!(window(WindowOutcome::WorkerDied).is_none());
    assert_eq!(death_cap(&ledger), Some(1));
    assert!(window(oom).is_none());
    let verdict = window(oom).expect("three failed one-item windows");
    assert_eq!(verdict.inference_id, "g/a");
    assert!(ledger.was_condemned("g/a", "CPU"));
}

/// While macOS is paging every window is one item with no room, so an
/// out-of-memory failure there says nothing about whether the model fits:
/// it neither counts toward [`OOM_WINDOWS_AT_FLOOR`] nor clears the count.
/// At warning without paging the room is real and the failure counts.
#[test]
fn an_oom_while_the_mac_pages_does_not_condemn_the_replica() {
    use mps::MemoryPressure::{Critical, Normal, Paging, Warning};
    for (pressed, counts) in [(Paging, false), (Critical, false), (Warning, true)] {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(9_900), Some(0));
        let admission = ledger
            .register_worker("g/big", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        let oom_window = |pressure| {
            ledger.set_memory_pressure_for_test(pressure);
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            assert_eq!(token.grant().unit_budget, 1);
            token.finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            })
        };
        // One short of the verdict.
        for _ in 1..OOM_WINDOWS_AT_FLOOR {
            assert!(oom_window(Normal).is_none());
        }
        if counts {
            assert!(oom_window(pressed).is_some(), "{pressed:?} counts");
            continue;
        }
        for _ in 0..(2 * OOM_WINDOWS_AT_FLOOR) {
            assert!(oom_window(pressed).is_none(), "{pressed:?} does not count");
        }
        assert!(
            oom_window(Normal).is_some(),
            "{pressed:?} did not clear the count either"
        );
    }
}

/// Once the model is resident its memory is ours and `external` falls, so a
/// one-item window gets a nominal share and still runs out of memory: the
/// room against one item's cost says the replica is at its floor.
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
            "priced, at a few hundred MiB: condemnation must not wait for \
             a window priced at nothing"
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

/// The same one-item out-of-memory on a card with **room to spare** deflates
/// and recovers, and no number of them condemns the replica.
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

/// The Windows sysmem fallback, whose 304 MiB of growth had 297 MiB of card
/// to grow into, against a heterogeneous-corpus drop whose growth all fitted
/// (docs/batch-calibration-design.md, "The worker's verdict is a candidate").
#[test]
fn the_two_measured_collapses_are_told_apart() {
    for (label, total_mb, free_mb, before_mb, peak_mb, units, rate, deflation) in [
        (
            "sysmem spill",
            32_607u64,
            297u64,
            41_374u64,
            41_678u64,
            8u64,
            0.278,
            1u32,
        ),
        ("fitted", 24_576, 20_975, 2_830, 5_762, 116, 13.0, 0),
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
/// and the size is neither a measured-clean floor nor a stored row.
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

/// A collapse is judged on this batch's figures only: a replica with no load
/// footprint does not deflate on a batch that over-committed nothing.
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
/// that released its blocks mid-batch reports a small after-figure.
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

/// A RAM-priced host is judged on available RAM against the RSS pool's
/// growth, not the load report's basis: a batch that grew 100 MiB with
/// 500 MiB available does not deflate.
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

/// On MPS the room a pool grows into is the machine's available RAM, the
/// domain [`VramLedger::external_locked`] sums in, not
/// `recommended_max_memory()`.
#[test]
fn a_collapse_on_a_unified_device_is_judged_in_the_ram_domain() {
    const TOTAL: u64 = 110_100;
    const BASE: u64 = 1_000;
    for (label, hog, pool, peak_mb, deflation) in [
        // 35 072 MiB of RAM is left under a 70 000 MiB hog: 15 500 MiB of
        // growth fits in it and 36 000 MiB does not.
        ("inside the room", 70_000u64, 25_000u64, 40_500u64, 0u32),
        ("past the room", 70_000, 25_000, 61_000, 1),
        // On an idle machine the free reading is clipped to
        // `recommended_max`, 14 972 MiB below the RAM this growth had.
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

/// A throughput collapse reported from a window a neighbour was running
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

    // Alone, the identical measurement is the WDDM spill signal.
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

/// A spill is a negative on the worker's own evidence (its pool exceeded the
/// GPU's used memory), with no rate drop or pool growth past free needed.
#[test]
fn a_spill_deflates_on_its_own_flag() {
    for (spilled, deflation, reason) in [(false, 0u32, None), (true, 1, Some("spill"))] {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                spilled,
                free_mb: Some(90_000),
                free_source: Some("nvml".to_owned()),
                ..warm_batch(64, 1.0)
            }]);
        let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
        assert_eq!(settled.window.expect("settled").negative_reason, reason);
        assert_eq!(
            ledger.health()[0].workers[0].deflation,
            deflation,
            "spilled = {spilled}"
        );
    }
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

/// A typed exception deflates without corroboration, whatever the free
/// reading says: a caching allocator can fail with gigabytes free and
/// fragmented.
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

/// An OOM classified from the failure's *wording* does not deflate when the
/// GPU's own reading at that instant still held the window's whole envelope.
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

/// A worker that states no class at all predates `oom_class`, and its bare
/// `oom` is trusted as before.
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
        ram_mb: 0,
        ram_bound: false,
        pressure: mps::MemoryPressure::Normal,
        item_cap: None,
        ram_only: false,
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

/// An MPS allocator failure at its own 5.38 GiB ceiling: reported as the
/// Mac's free RAM (103 918 MiB) the reading contradicts the grant; reported
/// as the allocator's headroom (5 505 - 4 911 = 594 MiB) it corroborates
/// the failure.
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
        ram_mb: 0,
        ram_bound: false,
        pressure: mps::MemoryPressure::Normal,
        item_cap: None,
        ram_only: false,
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

/// An out-of-memory negative names the tier that classified it.
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
        "the reading contradicts the wording, so this is no negative"
    );
    assert!(
        settled.oom.is_none(),
        "and a window that is not a negative has no tier to name; the \
         veto's own WARN is what speaks there"
    );
}

/// An error-frame OOM (no measurement to classify) is the host's own reading,
/// and the line credits the host.
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

/// A bare `oom` flag from a worker that predates `oom_class` deflates as
/// before, and the log says the tier is missing rather than guessing one.
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
    // Neither half of the CPU pair means anything on its own, and the pair
    // is per **line**, not per multi-line blob.
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
    // An impl wording an unrelated failure with the words.
    assert!(!message_reports_oom(
        "RuntimeError: refusing merged batch of 32: the caption cache is \
         out of memory slots"
    ));
    // Real wordings from shipped dependencies, which a closed spelling list
    // missed.
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
    // Per line: a Python traceback names `torch/cuda/__init__.py` in its
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
