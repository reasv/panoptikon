//! Host RAM on the CPU device: the reserve, and a replica's live footprint.
use super::*;
use crate::inferio::gpu::GpuInventory;

const GIB: u64 = 1024;

/// A CPU-only host with `ram_mb` of RAM. Free readings are the test's own.
fn cpu_host(ram_mb: u64, budget: VramBudget) -> Arc<VramLedger> {
    let mut ledger = VramLedger::new(&GpuInventory::known_cpu(ram_mb), budget.into(), None);
    Arc::get_mut(&mut ledger)
        .expect("not shared yet")
        .probe_external = false;
    ledger
}

/// A CPU worker's load report on a `ram_mb` host: `base_mb` grown across the
/// load, a resident set of `resident_mb` after it and a higher lifetime peak.
fn cpu_worker(ram_mb: u64, base_mb: u64, resident_mb: u64) -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(base_mb),
        base_method: Some("rss".to_owned()),
        reserved_at_load_mb: Some(resident_mb + 300),
        allocated_at_load_mb: Some(resident_mb),
        gpu_total_mb: Some(ram_mb),
        device_kind: Some("cpu".to_owned()),
        torch_version: Some("2.7.1+cpu".to_owned()),
        ..LoadReport::default()
    }));
    Arc::new(StdMutex::new(telemetry))
}

/// A CPU worker's memory sample: free RAM, its lifetime peak resident set
/// (`reserved`) and what it holds now (`allocated`).
fn push_cpu_sample(handle: &TelemetryHandle, ram_mb: u64, free_mb: u64, peak_mb: u64, now_mb: u64) {
    handle.lock().unwrap().memory = Some(Timestamped::now(MemorySample {
        free_mb: Some(free_mb),
        total_mb: Some(ram_mb),
        free_source: Some("ram".to_owned()),
        reserved_mb: Some(peak_mb),
        allocated_mb: Some(now_mb),
        ..MemorySample::default()
    }));
}

/// A fit that prices a grant at `mb_per_unit` under the default pool margin.
fn price(ledger: &VramLedger, model: &str, mb_per_unit: f64) {
    ledger.install_fit_for_test(
        model,
        cpu::DEVICE_KEY,
        FitSnapshot {
            slope_mb_per_unit: mb_per_unit / POOL_MARGIN_DEFAULT,
            intercept_mb: 0.0,
            residual_mb: 0.0,
            samples: 8,
            version: 1,
        },
    );
}

/// The reserve is a tenth of the machine between 2 and 16 GiB, with the
/// margin unset or set to 0. A larger configured margin raises it.
#[test]
fn the_ram_reserve_scales_with_the_machine() {
    for (ram_mb, reserve) in [
        (4 * GIB, 2 * GIB),
        (8 * GIB, 2 * GIB),
        (16 * GIB, 2 * GIB),
        (32 * GIB, 3_276),
        (64 * GIB, 6_553),
        (128_649, 12_864),
        (256 * GIB, 16 * GIB),
        (1024 * GIB, 16 * GIB),
    ] {
        assert_eq!(cpu::ram_reserve_mb(ram_mb), reserve, "{ram_mb}");
        for budget in [VramBudget::default(), user_margin(0.0), user_margin(0.01)] {
            let ledger = cpu_host(ram_mb, budget);
            ledger.record_free_for_test(cpu::DEVICE_KEY, ram_mb / 2);
            let row = &ledger.health()[0];
            assert_eq!(row.reserve_mb, reserve, "{ram_mb} {budget:?}");
            assert_eq!(row.reserve_rule, RESERVE_RULE_RAM_FLOOR);
            assert_eq!(row.limit_mb, ram_mb / 2 - reserve, "free less the reserve");
        }
    }
    // Half of what other processes use is more than a tenth of the machine.
    let ledger = cpu_host(64 * GIB, user_margin(0.5));
    ledger.record_free_for_test(cpu::DEVICE_KEY, 24 * GIB);
    let row = &ledger.health()[0];
    assert_eq!(row.external_mb, 40 * GIB);
    assert_eq!(row.reserve_mb, 20 * GIB);
    assert_eq!(row.reserve_rule, RESERVE_RULE_USER_MARGIN);
}

/// On a small host the reserve leaves less to grant but refuses no load and
/// stalls no model: with no headroom a window still runs one unit at a time.
#[test]
fn a_small_host_still_runs_under_the_reserve() {
    // (RAM, used by others, model base, units granted at 20 MiB each)
    for (ram_mb, others_mb, base_mb, units) in [
        // 7 800 − 3 000 − 2 048 = 2 752 limit; 1 252 above the base.
        (7_800, 3_000, 1_500, 62),
        // 15 900 − 5 000 − 2 048 = 8 852 limit; 5 852 above the base.
        (15_900, 5_000, 3_000, 292),
        // 7 800 − 1 000 − 2 048 = 4 752 limit, below the base.
        (7_800, 1_000, 5_000, 1),
    ] {
        let ledger = cpu_host(ram_mb, VramBudget::default());
        {
            let state = ledger.lock();
            assert_eq!(
                ledger.refusal_room_locked(&state, cpu::DEVICE_KEY),
                ram_mb * 3 / 4,
                "a load is refused against the cap alone, as without a reserve"
            );
        }
        let handle = cpu_worker(ram_mb, base_mb, base_mb);
        let admission = ledger
            .register_worker("g/small", item_cost(1024), &handle, None)
            .expect("admitted");
        price(&ledger, "g/small", 20.0);
        push_cpu_sample(
            &handle,
            ram_mb,
            ram_mb - others_mb - base_mb,
            base_mb,
            base_mb,
        );
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, units, "{ram_mb} {base_mb}");
        assert_eq!(token.grant().ram_reserve_mb, 2 * GIB);
        let row = &ledger.health()[0];
        assert_eq!(row.external_mb, others_mb);
        assert_eq!(row.limit_mb, ram_mb - others_mb - 2 * GIB);
    }
}

/// A 128 GiB host where other processes hold most of the RAM and the worker
/// has just given a 20 GiB batch back: the next grant is what is free above
/// the reserve, and none of the returned memory is counted twice.
#[test]
fn a_grant_on_a_busy_host_keeps_the_reserve_free() {
    const RAM_MB: u64 = 128_649;
    const MB_PER_UNIT: f64 = 47.57;
    let ledger = cpu_host(RAM_MB, VramBudget::default());
    let handle = cpu_worker(RAM_MB, 290, 799);
    let admission = ledger
        .register_worker("g/ocr", item_cost(768), &handle, None)
        .expect("admitted");
    price(&ledger, "g/ocr", MB_PER_UNIT);
    // 25 471 MiB free once reclaimable slab is left out; the worker peaked at
    // 20 472 MiB and holds 1 200 now.
    push_cpu_sample(&handle, RAM_MB, 25_471, 20_472, 1_200);

    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let row = &ledger.health()[0];
    assert_eq!(row.workers[0].footprint_mb, 290 + (1_200 - 799));
    assert_eq!(row.external_mb, RAM_MB - 25_471 - 691);
    assert_eq!(
        (row.reserve_mb, row.reserve_rule.as_str()),
        (12_864, "ram_floor")
    );
    assert_eq!(row.limit_mb, 25_471 + 691 - 12_864);
    let grant = *token.grant();
    // 25 471 free − 12 864 reserve = 12 607 MiB: 265 units, where the ramp
    // asked for 768 (36 534 MiB).
    assert_eq!((grant.unit_budget, grant.mb), (265, 12_607));
    assert_eq!(grant.ram_reserve_mb, 12_864);
    assert!(grant.squeezed);
}

/// A CPU replica's footprint is the memory it holds now. What a batch gave
/// back is in the free reading, so it is no room for another grant, and a
/// window in flight is charged in full beside it.
#[test]
fn a_cpu_replica_is_charged_what_it_holds_and_what_it_was_granted() {
    const RAM_MB: u64 = 64 * GIB;
    let ledger = cpu_host(RAM_MB, VramBudget::default());
    let (a_handle, b_handle) = (cpu_worker(RAM_MB, 500, 500), cpu_worker(RAM_MB, 500, 500));
    let a = ledger
        .register_worker("g/a", item_cost(4096), &a_handle, None)
        .expect("admitted");
    let b = ledger
        .register_worker("g/b", item_cost(4096), &b_handle, None)
        .expect("admitted");
    for model in ["g/a", "g/b"] {
        price(&ledger, model, 10.0);
    }
    // A ran a 30 GiB batch and gave it back; 40 GiB is free.
    push_cpu_sample(&a_handle, RAM_MB, 40 * GIB, 30 * GIB, 700);
    push_cpu_sample(&b_handle, RAM_MB, 40 * GIB, 800, 500);
    ledger.ingest_all_for_test();
    let row = &ledger.health()[0];
    let a_row = &row.workers[0];
    assert_eq!((a_row.reserved_mb, a_row.footprint_mb), (Some(700), 700));
    assert_eq!(row.footprints_mb, 700 + 500);
    assert_eq!(row.external_mb, RAM_MB - 40 * GIB - 1_200);
    let headroom = 40 * GIB - 6_553;
    assert_eq!(row.headroom_mb, headroom);

    // A's next window takes the whole headroom and no more.
    let first = a.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(first.grant().mb, headroom / 10 * 10);
    assert_eq!(ledger.health()[0].charges_mb, 1_200 + first.grant().mb);
    // While it runs, B is left the one-unit floor.
    let second = b.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(second.grant().unit_budget, 1);
    assert!(second.grant().mb < 10);
}

/// The per-batch level a CPU worker reports replaces its peak as the pool.
#[test]
fn a_cpu_batch_reports_the_resident_set_it_left() {
    const RAM_MB: u64 = 64 * GIB;
    let ledger = cpu_host(RAM_MB, VramBudget::default());
    let handle = cpu_worker(RAM_MB, 500, 500);
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .expect("admitted");
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            reserved_after_mb: Some(9_000),
            rss_after_mb: Some(650),
            ..measurement(8, 800, 9_000)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.reserved_at_load_mb, Some(500));
    assert_eq!(worker.reserved_mb, Some(650));
    assert_eq!(worker.footprint_mb, 500 + 150);
}
