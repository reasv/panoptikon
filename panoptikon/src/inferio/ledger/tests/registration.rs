//! Registration: which GPU a worker is admitted under, and when it is refused.
use super::*;

/// `none`-class models, workers with no GPU at all, and GPUs outside
/// the inventory get no admission — they take the unpriced dispatch path.
#[test]
fn unadmissible_replicas_get_no_handle() {
    let ledger = ledger(10_000, VramBudget::default());
    let none_class = CostDimension {
        unit: CostUnit::None,
        aggregation: None,
        epoch: 1,
        seed_units: None,
        degraded: false,
        canvas_pixels: None,
        max_tokens: None,
    };
    assert!(
        ledger
            .register_worker("g/api", none_class, &loaded(Some(10), Some(0)), None)
            .is_none(),
        "the none class is never priced"
    );
    let bare: TelemetryHandle = Arc::new(StdMutex::new(WorkerTelemetry::default()));
    assert!(
        ledger
            .register_worker("g/a", item_cost(4), &bare, None)
            .is_none(),
        "no load report at all (no torch, CPU/MPS host)"
    );
    let mut elsewhere = WorkerTelemetry::default();
    elsewhere.load = Some(Timestamped::now(LoadReport {
        gpu_uuid: Some("GPU-elsewhere".to_owned()),
        base_mb: Some(100),
        ..LoadReport::default()
    }));
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                &Arc::new(StdMutex::new(elsewhere)),
                None
            )
            .is_none(),
        "a GPU the inventory does not list"
    );
}

/// A two-GPU ROCm-shaped ledger: keys in `GPU-BDF-…` form, a PCI
/// address per GPU, and 24 GB cards.
fn rocm_ledger() -> Arc<VramLedger> {
    VramLedger::for_test_gpus(
        &[
            (AMD_A, "AMD gfx1100 (24 GB)", 24_576, Some("0000:03:00.0")),
            (AMD_B, "AMD gfx1100 (24 GB)", 24_576, Some("0000:0c:00.0")),
        ],
        VramBudget::default(),
        None,
    )
}

/// The ROCm path: with no UUID, the worker's PCI address is the join, accepted
/// only once the worker's own total-VRAM reading agrees with the GPU's.
#[test]
fn a_bdf_match_admits_under_the_gpus_key() {
    let ledger = rocm_ledger();
    // 24_560 against 24_576: the ordinary few-MB driver-reserve skew.
    let handle = loaded_rocm(Some("0000:0c:00.0"), Some(24_560));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    assert_eq!(
        admitted_gpu(&ledger, 0),
        (AMD_B.to_owned(), "g/a".to_owned()),
        "admitted under the second GPU's key, from its address alone"
    );
    // The address is compared case-insensitively: sysfs and torch render
    // hex independently and neither side promises a case.
    let ledger = rocm_ledger();
    let upper = loaded_rocm(Some("0000:0C:00.0"), Some(24_576));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &upper, None)
        .expect("admitted");
    assert_eq!(admitted_gpu(&ledger, 0).0, AMD_B);
}

/// A BDF match whose totals disagree, or that cannot be checked at all, is
/// refused rather than priced against a GPU the worker may not be on.
#[test]
fn a_bdf_match_is_refused_without_an_agreeing_total() {
    let ledger = rocm_ledger();
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                // A 16 GB GPU reported against a 24 GB row: the
                // enumeration is wrong somewhere.
                &loaded_rocm(Some("0000:03:00.0"), Some(16_384)),
                None
            )
            .is_none(),
        "totals disagree by far more than the tolerance"
    );
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:03:00.0"), None),
                None
            )
            .is_none(),
        "no total at all cannot pass a check, and only an exact UUID \
         match is admitted without one"
    );
    assert!(
        ledger.health().iter().all(|gpu| gpu.workers.is_empty()),
        "nothing was admitted"
    );
    // The tolerance is max(5%, 512 MB): 24_576 * 5% = 1228 MB.
    let ledger = rocm_ledger();
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:03:00.0"), Some(24_576 - 1200)),
                None
            )
            .is_some(),
        "inside 5%"
    );
    let ledger = rocm_ledger();
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:03:00.0"), Some(24_576 - 1300)),
                None
            )
            .is_none(),
        "outside 5%"
    );
}

/// The whole ROCm shape, wire to GPU: a msgpack `load` payload with no
/// `gpu_uuid`, a PCI address, torch's own total, `base_method: "fdinfo"` and
/// an `"amdgpu-sysfs"` memory sample, decoded and registered.
#[test]
fn a_rocm_wire_load_report_reaches_the_gpu_it_names() {
    use rmpv::Value;

    let payload = vec![
        (Value::from("base_mb"), Value::from(2048u64)),
        (Value::from("base_method"), Value::from("fdinfo")),
        (Value::from("reserved_at_load_mb"), Value::from(1800u64)),
        (Value::from("dtype"), Value::from("fp16")),
        (Value::from("gpu_bdf"), Value::from("0000:0c:00.0")),
        (Value::from("gpu_total_mb"), Value::from(24_560u64)),
        (Value::from("gpu_integrated"), Value::from(false)),
        (
            Value::from("gpu_name"),
            Value::from("AMD Radeon RX 7900 XTX"),
        ),
        (Value::from("torch_version"), Value::from("2.11.0+rocm7.2")),
        (
            Value::from("memory"),
            Value::Map(vec![
                (Value::from("free_mb"), Value::from(21_000u64)),
                (Value::from("total_mb"), Value::from(24_560u64)),
                (Value::from("free_source"), Value::from("amdgpu-sysfs")),
                (Value::from("reserved_mb"), Value::from(1800u64)),
                (Value::from("allocated_mb"), Value::from(1500u64)),
            ]),
        ),
    ];
    let report = LoadReport::parse(&payload).expect("a ROCm load report");
    assert_eq!(report.gpu_uuid, None, "suppressed on HIP");
    assert_eq!(report.base_method.as_deref(), Some("fdinfo"));
    assert_eq!(report.gpu_integrated, Some(false));

    // A worker whose HIP runtime calls the discrete GPU integrated is
    // admitted all the same, and the disagreement is logged once per GPU.
    for (integrated, logged) in [(true, 1), (false, 0)] {
        let ledger = rocm_ledger();
        let mut report = report.clone();
        report.gpu_integrated = Some(integrated);
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(report));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        for model in ["g/a", "g/b"] {
            ledger
                .register_worker(model, item_cost(4), &handle, None)
                .expect("admitted");
        }
        assert_eq!(ledger.lock().integrated_mismatch_logged.len(), logged);
    }

    let ledger = rocm_ledger();
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(report));
    let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted by address, cross-checked by total");
    assert_eq!(
        admitted_gpu(&ledger, 0),
        (AMD_B.to_owned(), "g/a".to_owned())
    );
    assert_eq!(
        ledger
            .lock()
            .workers
            .values()
            .next()
            .and_then(|worker| worker.base_method.clone())
            .as_deref(),
        Some("fdinfo"),
        "the provenance the calibration profile is written with"
    );

    // The load response's own sample is recorded at once under its own
    // source, which a later `"torch"` reading cannot displace.
    let sourced = |ledger: &Arc<VramLedger>| {
        ledger
            .health()
            .into_iter()
            .find(|gpu| gpu.gpu_uuid == AMD_B)
            .expect("the GPU the worker named")
            .external_source
    };
    assert_eq!(sourced(&ledger).as_deref(), Some("amdgpu-sysfs"));

    {
        let mut telemetry = handle.lock().unwrap();
        telemetry.memory = Some(Timestamped::now(MemorySample {
            free_mb: Some(9_000),
            total_mb: Some(24_560),
            free_source: Some("torch".to_owned()),
            reserved_mb: Some(1800),
            allocated_mb: Some(1500),
            ..MemorySample::default()
        }));
    }
    ledger.ingest_all_for_test();
    assert_eq!(
        sourced(&ledger).as_deref(),
        Some("amdgpu-sysfs"),
        "a torch reading does not displace the whole-GPU one"
    );
}

/// A PCI address no GPU in the inventory has means the worker is on a GPU
/// this inventory does not describe. It must not fall back to anything.
#[test]
fn a_bdf_outside_the_inventory_is_refused() {
    let ledger = rocm_ledger();
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:41:00.0"), Some(24_576)),
                None
            )
            .is_none()
    );
    // Not even on a single-GPU host: the address is evidence of the *wrong*
    // GPU, which is not the same as no evidence.
    let single = VramLedger::for_test_gpus(
        &[(AMD_A, "AMD gfx1100 (24 GB)", 24_576, Some("0000:03:00.0"))],
        VramBudget::default(),
        None,
    );
    assert!(
        single
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:41:00.0"), Some(24_576)),
                None
            )
            .is_none()
    );
}

/// A UUID that matches **no** GPU does not end the search: a MIG instance or
/// a restricted CUDA inventory still has a PCI address to match.
#[test]
fn a_uuid_that_matches_nothing_falls_through_to_the_bdf() {
    let ledger = rocm_ledger();
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1000),
        gpu_uuid: Some("GPU-a-third-vocabulary".to_owned()),
        gpu_bdf: Some("0000:03:00.0".to_owned()),
        gpu_total_mb: Some(24_576),
        ..LoadReport::default()
    }));
    let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted on the address");
    assert_eq!(admitted_gpu(&ledger, 0).0, AMD_A);
}

/// The NVML single-GPU fallback's twin: one GPU, nothing matched, and no address
/// that *could* have matched (a CUDA inventory carries none).
#[test]
fn the_single_gpu_fallback_needs_an_agreeing_total() {
    let bare = |total: Option<u64>| {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            gpu_total_mb: total,
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry)) as TelemetryHandle
    };
    let single = ledger(24_576, VramBudget::default());
    let _admission = single
        .register_worker("g/a", item_cost(4), &bare(Some(24_400)), None)
        .expect("one GPU, and the worker's own total says it is that GPU");
    assert_eq!(admitted_gpu(&single, 0).0, GPU);

    let fresh = ledger(24_576, VramBudget::default());
    assert!(
        fresh
            .register_worker("g/a", item_cost(4), &bare(Some(8192)), None)
            .is_none(),
        "a GPU a third the size is not this one"
    );
    assert!(
        fresh
            .register_worker("g/a", item_cost(4), &bare(None), None)
            .is_none(),
        "and an unverifiable claim is not admitted"
    );
    // A report that says nothing about a GPU at all (a CPU impl that
    // imported torch, a remote API) is not a candidate, not a failed match.
    let mut cpu = WorkerTelemetry::default();
    cpu.load = Some(Timestamped::now(LoadReport {
        torch_version: Some("2.7.1+cu128".to_owned()),
        ..LoadReport::default()
    }));
    assert!(
        fresh
            .register_worker("g/a", item_cost(4), &Arc::new(StdMutex::new(cpu)), None)
            .is_none()
    );

    let two = VramLedger::for_test_gpus(
        &[
            (GPU, "TEST 9000", 24_576, None),
            ("GPU-bbbb", "TEST 9000", 24_576, None),
        ],
        VramBudget::default(),
        None,
    );
    assert!(
        two.register_worker("g/a", item_cost(4), &bare(Some(24_576)), None)
            .is_none(),
        "two identical GPUs: the total identifies neither"
    );
}

/// A ROCm replica's pin is a HIP index while its ledger key is the device
/// key, so a load reservation taken with the pin string must be resolved.
#[tokio::test]
async fn a_rocm_index_pin_reserves_against_the_gpu_it_names() {
    let amd = |index: u32, bdf: &str| crate::inferio::gpu::GpuInfo {
        index,
        uuid: format!("GPU-BDF-{bdf}"),
        name: "AMD gfx1100 (24 GB)".to_owned(),
        total_mb: 24_576,
        compute_cap: None,
        bdf: Some(bdf.to_owned()),
        gfx_target_version: Some(110_000),
        unified_ram_mb: None,
        vram_carveout_mb: None,
    };
    let inventory = GpuInventory::known_rocm(vec![amd(0, "0000:03:00.0"), amd(1, "0000:0c:00.0")]);
    let ledger = VramLedger::new(&inventory, VramBudget::default().into(), None);
    // `reserve_load`'s load-path probe would otherwise read this machine's
    // sysfs about two synthetic PCI addresses.
    ledger.install_probe_stub(None);
    let pin = inventory.resolve_pin(Some("1")).expect("a HIP index");
    assert_eq!(pin, "1");
    assert!(
        ledger
            .reserve_load_for_test("g/a", item_cost(4), &pin, None)
            .await
            .is_none(),
        "the pin alone names no ledger GPU — this was the gap"
    );
    let key = inventory
        .resolve_device_key(Some("1"))
        .expect("the same request in the ledger's vocabulary");
    assert_eq!(key, AMD_B);
    let reservation = ledger
        .reserve_load_for_test("g/a", item_cost(4), &key, None)
        .await;
    assert!(reservation.is_some(), "and the pair does");
    // The reservation lands on the GPU the pin selected, not the other.
    let charged = |uuid: &str| {
        ledger
            .health()
            .into_iter()
            .find(|gpu| gpu.gpu_uuid == uuid)
            .map(|gpu| gpu.load_reservations_mb)
            .unwrap()
    };
    assert!(charged(AMD_B) > 0, "the pinned GPU carries the charge");
    assert_eq!(charged(AMD_A), 0);
    drop(reservation);
    assert_eq!(charged(AMD_B), 0, "and gives it back when the load ends");
}

/// The inventory's PCI addresses must reach the ledger through
/// `VramLedger::new`, or the BDF arm refuses every ROCm replica.
#[test]
fn the_ledger_carries_the_inventorys_pci_addresses() {
    // Two GPUs: on a single-GPU host the total-only fallback would admit the
    // replica even without the PCI addresses.
    let amd = |index: u32, bdf: &str, total_mb: u64| crate::inferio::gpu::GpuInfo {
        index,
        uuid: format!("GPU-BDF-{bdf}"),
        name: "AMD gfx1100 (24 GB)".to_owned(),
        total_mb,
        compute_cap: None,
        bdf: Some(bdf.to_owned()),
        gfx_target_version: Some(110_000),
        unified_ram_mb: None,
        vram_carveout_mb: None,
    };
    let inventory = GpuInventory::known_rocm(vec![
        amd(0, "0000:03:00.0", 24_576),
        amd(1, "0000:0c:00.0", 16_368),
    ]);
    let ledger = VramLedger::new(&inventory, VramBudget::default().into(), None);
    let handle = loaded_rocm(Some("0000:03:00.0"), Some(24_576));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("the address reached the ledger");
    assert_eq!(admitted_gpu(&ledger, 0).0, AMD_A);
}

/// An index-form `CUDA_VISIBLE_DEVICES` is unmappable, but the load report
/// names the GPU by UUID, so the ledger adopts that row and prices it.
#[test]
fn an_index_mask_prices_the_gpu_the_first_load_report_names() {
    let profiles = Arc::new(FakeProfiles::default());
    let inventory = GpuInventory::masked(vec![
        nvidia(0, "GPU-1a2b", "TEST 9000", 32_607),
        nvidia(1, "GPU-3c4d", "TEST 9001", 100_000),
    ]);
    assert!(inventory.gpus().is_none(), "the mask still blanks it");
    let ledger = VramLedger::new(
        &inventory,
        no_margin().into(),
        Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
    );
    ledger.install_probe_stub(None);
    assert!(
        ledger.health().is_empty(),
        "nothing is priced before a load"
    );

    // The worker CUDA put on index 0 reports the second nvidia-smi row.
    let handle = loaded_on("GPU-3c4d", Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("the reported GPU is adopted and priced");
    push_memory(&handle, 90_000, 0);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert!(token.grant().unit_budget > 0, "grants are issued");
    drop(token);
    measured_window(&handle, &admission, 8);

    let health = ledger.health();
    assert_eq!(health.len(), 1, "only the GPU a worker reported");
    assert_eq!(health[0].gpu_uuid, "GPU-3c4d");
    assert_eq!(
        health[0].total_mb, 100_000,
        "the row's own total, not a guess"
    );
    let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        written.gpu_name, "TEST 9001",
        "the store row is that card's"
    );
    assert_eq!(written.arch, ARCH);
}

/// Adoption is keyed on a UUID nvidia-smi listed, so it cannot invent a GPU:
/// a worker on a device this host never reported stays unpriced, and the
/// operator is told once, at WARN, with the remedy.
#[test]
fn a_gpu_no_inventory_row_names_stays_unadmitted() {
    let inventory = GpuInventory::masked(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
    let ledger = VramLedger::new(&inventory, no_margin().into(), None);
    ledger.install_probe_stub(None);
    let handle = loaded_on("MIG-9f9f", Some(1000), Some(0));
    assert!(
        ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .is_none(),
        "no row names this device"
    );
    assert!(ledger.health().is_empty());
    assert!(
        ledger.lock().unpriced_warned.contains("MIG-9f9f"),
        "and the WARN fired — a GPU worker running unpriced is not a debug line"
    );
    // Said once: the remedy is a host fact, and loads repeat.
    let second = loaded_on("MIG-9f9f", Some(1000), Some(0));
    let resolution = {
        let mut state = ledger.lock();
        let report = second.lock().unwrap().load.clone().unwrap().value;
        let refused = VramLedger::resolve_gpu(&state, &report, None);
        VramLedger::escalate_first_unpriced(&mut state, refused, &report)
    };
    assert!(
        matches!(resolution.log, Some(GpuLog::NoGpu { .. })),
        "the second refusal is the debug line again"
    );
}

/// The WARN's guard is per **reported GPU**: a respawn on the same card is
/// silent, and a second unadmitted card gets its own line.
#[test]
fn the_unpriced_warn_is_once_per_reported_gpu() {
    let inventory = GpuInventory::masked(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
    let ledger = VramLedger::new(&inventory, no_margin().into(), None);
    ledger.install_probe_stub(None);
    let warns = |gpu: &str| {
        let handle = loaded_on(gpu, Some(1000), Some(0));
        let report = handle.lock().unwrap().load.clone().unwrap().value;
        let mut state = ledger.lock();
        let refused = VramLedger::resolve_gpu(&state, &report, None);
        let out = VramLedger::escalate_first_unpriced(&mut state, refused, &report);
        matches!(out.log, Some(GpuLog::UnadmittedGpuWorker { .. }))
    };
    assert!(warns("MIG-9f9f"), "the first refusal on a card warns");
    for _ in 0..5 {
        assert!(!warns("MIG-9f9f"), "every respawn on it is silent");
    }
    assert!(warns("GPU-ffff"), "a second unadmitted card warns too");
    assert!(!warns("GPU-ffff"), "and then goes quiet as well");
    assert_eq!(ledger.lock().unpriced_warned.len(), 2);
}

/// A load report with no GPU facts at all: the CPU-built worker.
fn loaded_without_a_device() -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1000),
        base_method: Some("rss".to_owned()),
        ..LoadReport::default()
    }));
    Arc::new(StdMutex::new(telemetry))
}

/// A worker that names **no device at all** (an impl that never imported
/// torch, or an older worker) cannot be placed: the first refusal is a WARN
/// and every repeat is DEBUG.
#[test]
fn a_worker_that_names_no_device_warns_once() {
    let refuse = |ledger: &Arc<VramLedger>| {
        let handle = loaded_without_a_device();
        let report = handle.lock().unwrap().load.clone().unwrap().value;
        let mut state = ledger.lock();
        let refused = VramLedger::resolve_gpu(&state, &report, None);
        VramLedger::escalate_first_unpriced(&mut state, refused, &report).log
    };

    let gpu_host = ledger(32_607, no_margin());
    let first = refuse(&gpu_host);
    assert!(
        matches!(first, Some(GpuLog::UnadmittedDevicelessWorker { gpus: 1 })),
        "the first refusal is the escalation"
    );
    for _ in 0..3 {
        assert!(
            matches!(refuse(&gpu_host), Some(GpuLog::NoGpu { .. })),
            "and it is said once"
        );
    }

    // Having a CPU device changes nothing: this worker did not say it ran
    // on the CPU, and a device-less report is as unplaceable there.
    let cpu_host = VramLedger::for_test(
        &[(crate::inferio::cpu::DEVICE_KEY, "CPU (128 GB)", 128_649)],
        no_margin(),
    );
    assert!(
        matches!(
            refuse(&cpu_host),
            Some(GpuLog::UnadmittedDevicelessWorker { .. })
        ),
        "it can be placed nowhere here either"
    );

    // A worker that *does* name the CPU is admitted on that device and
    // never reaches the escalation.
    let handle = loaded_on_cpu(Some(128_649));
    assert!(
        cpu_host
            .register_worker("g/a", item_cost(4), &handle, None)
            .is_some()
    );

    let logs = captured_logs(|| GpuLog::UnadmittedDevicelessWorker { gpus: 1 }.emit("g/a"));
    assert_eq!(logs[0].0, tracing::Level::WARN);
    assert!(
        logs[0].1.contains("names no device at all"),
        "the WARN says what is wrong: {}",
        logs[0].1
    );
}

/// The levels the masked-adoption path logs at: a GPU worker dispatched
/// unpriced is a WARN, while the ordinary "reports no GPU" refusal of CPU,
/// MPS and remote replicas stays at DEBUG.
#[test]
fn the_unadmitted_gpu_worker_line_is_a_warn_and_the_plain_refusal_is_not() {
    let logs = captured_logs(|| {
        GpuLog::UnadmittedGpuWorker {
            worker_uuid: Some("MIG-9f9f".to_owned()),
            worker_bdf: None,
            gpus: 0,
            adoptable: 1,
        }
        .emit("g/a");
        GpuLog::NoGpu {
            worker_uuid: None,
            worker_bdf: None,
            gpus: 0,
        }
        .emit("g/b");
        GpuLog::MaskedGpuAdopted {
            gpu: "GPU-1a2b".to_owned(),
            name: "TEST 9000".to_owned(),
            total_mb: 32_607,
            adoptable: 0,
        }
        .emit("g/c");
    });
    let levels: Vec<tracing::Level> = logs.iter().map(|(level, _)| *level).collect();
    assert_eq!(
        levels,
        vec![
            tracing::Level::WARN,
            tracing::Level::DEBUG,
            tracing::Level::INFO
        ]
    );
    assert!(
        logs[0].1.contains("dispatched without VRAM admission"),
        "the WARN carries the reason: {}",
        logs[0].1
    );
    assert!(
        logs[0].1.contains("nvidia-smi -L"),
        "and the remedy: {}",
        logs[0].1
    );
}

/// A UUID-form mask resolves statically: the hidden card is not in the
/// inventory and is not adoptable either.
#[test]
fn a_uuid_mask_adopts_nothing() {
    let inventory = GpuInventory::known(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
    assert!(inventory.adoptable().is_empty());
    let ledger = VramLedger::new(&inventory, no_margin().into(), None);
    ledger.install_probe_stub(None);
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_on("GPU-3c4d", Some(1000), Some(0)),
                None
            )
            .is_none(),
        "the masked-out card stays outside the ledger"
    );
    let handle = loaded_on("GPU-1a2b", Some(1000), Some(0));
    assert!(
        ledger
            .register_worker("g/b", item_cost(4), &handle, None)
            .is_some(),
        "and the visible one is priced as before"
    );
    assert_eq!(ledger.health().len(), 1);
}

/// A masked two-card host, as `build` produces under an index-form
/// `CUDA_VISIBLE_DEVICES`: the inventory is unknown, both rows adoptable.
fn masked_pair() -> GpuInventory {
    GpuInventory::masked(vec![
        nvidia(0, "GPU-1a2b", "TEST 9000", 32_607),
        nvidia(1, "GPU-3c4d", "TEST 9001", 100_000),
    ])
}

/// A load report from a worker whose torch is too old to expose
/// `get_device_properties().uuid`: no UUID, but a total.
fn uuidless_report(total_mb: u64) -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1000),
        base_method: Some("nvml".to_owned()),
        reserved_at_load_mb: Some(0),
        allocated_at_load_mb: Some(0),
        gpu_uuid: None,
        gpu_total_mb: Some(total_mb),
        gpu_arch: Some(ARCH.to_owned()),
        torch_version: Some("2.7.1+cu128".to_owned()),
        dtype: Some("fp16".to_owned()),
        ..LoadReport::default()
    }));
    Arc::new(StdMutex::new(telemetry))
}

/// Two workers on different cards under one index-form mask each adopt
/// their own row, with their own totals. No card is priced twice.
#[test]
fn two_workers_on_different_cards_adopt_one_row_each() {
    let ledger = VramLedger::new(&masked_pair(), no_margin().into(), None);
    ledger.install_probe_stub(None);
    let a = loaded_on("GPU-1a2b", Some(1000), Some(0));
    let b = loaded_on("GPU-3c4d", Some(1000), Some(0));
    let _a = ledger
        .register_worker("g/a", item_cost(4), &a, None)
        .expect("the first card is adopted");
    let _b = ledger
        .register_worker("g/b", item_cost(4), &b, None)
        .expect("the second card is adopted");
    let mut health = ledger.health();
    health.sort_by(|x, y| x.gpu_uuid.cmp(&y.gpu_uuid));
    assert_eq!(health.len(), 2);
    assert_eq!(
        (health[0].gpu_uuid.as_str(), health[0].total_mb),
        ("GPU-1a2b", 32_607)
    );
    assert_eq!(
        (health[1].gpu_uuid.as_str(), health[1].total_mb),
        ("GPU-3c4d", 100_000)
    );
    assert_eq!(health[0].workers.len(), 1);
    assert_eq!(health[1].workers.len(), 1);
    assert!(ledger.lock().adoptable.is_empty(), "each row moved once");
}

/// Two replicas of one model on one card adopt that card once and leave
/// the other alone.
#[test]
fn two_replicas_on_one_card_adopt_it_once() {
    let ledger = VramLedger::new(&masked_pair(), no_margin().into(), None);
    ledger.install_probe_stub(None);
    let a = loaded_on("GPU-3c4d", Some(1000), Some(0));
    let b = loaded_on("GPU-3c4d", Some(1000), Some(0));
    let _a = ledger
        .register_worker("g/a", item_cost(4), &a, None)
        .expect("first replica");
    assert_eq!(ledger.lock().adoptable.len(), 1);
    let _b = ledger
        .register_worker("g/a", item_cost(4), &b, None)
        .expect("second replica");
    assert_eq!(ledger.lock().adoptable.len(), 1, "no second adoption");
    let health = ledger.health();
    assert_eq!(health.len(), 1);
    assert_eq!(health[0].workers.len(), 2, "both priced against one card");
}

/// A replica respawned on the other card: adoption belongs to the ledger's
/// GPU set, so both cards end up priced and the first keeps what it learned.
#[test]
fn a_respawn_on_the_other_card_adopts_it_too_and_keeps_the_first() {
    let ledger = VramLedger::new(&masked_pair(), no_margin().into(), None);
    ledger.install_probe_stub(None);
    let first = loaded_on("GPU-1a2b", Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &first, None)
        .expect("adopted");
    drop(admission);
    let second = loaded_on("GPU-3c4d", Some(1000), Some(0));
    let _second = ledger
        .register_worker("g/a", item_cost(4), &second, None)
        .expect("the respawn's card is adopted too");
    let mut health = ledger.health();
    health.sort_by(|x, y| x.gpu_uuid.cmp(&y.gpu_uuid));
    assert_eq!(health.len(), 2, "the first adoption is never undone");
    assert!(health[0].workers.is_empty(), "the dead replica is gone");
    assert_eq!(health[1].workers.len(), 1);
    assert!(ledger.lock().adoptable.is_empty());
}

/// A second worker on the card the mask still hides, reporting a total but
/// **no UUID**, must not be admitted against the first card's budget: the
/// single-GPU fallback stands down while any row is still adoptable.
#[test]
fn a_uuidless_report_is_refused_while_another_card_is_adoptable() {
    let inventory = GpuInventory::masked(vec![
        nvidia(0, "GPU-1a2b", "TEST 9000", 32_607),
        nvidia(1, "GPU-3c4d", "TEST 9000", 32_607),
    ]);
    let ledger = VramLedger::new(&inventory, no_margin().into(), None);
    ledger.install_probe_stub(None);
    let a = loaded_on("GPU-1a2b", Some(1000), Some(0));
    let _a = ledger
        .register_worker("g/a", item_cost(4), &a, None)
        .expect("adopted");
    let b = uuidless_report(32_607);
    assert!(
        ledger
            .register_worker("g/b", item_cost(4), &b, None)
            .is_none(),
        "an unidentifiable report is not priced against someone else's card"
    );
    let health = ledger.health();
    assert_eq!(health.len(), 1);
    assert_eq!(health[0].workers.len(), 1, "only the card's own replica");
    assert_eq!(ledger.lock().adoptable.len(), 1, "GPU-3c4d is still hidden");
}

/// On a host with nothing adoptable, the same fallback still admits the
/// UUID-less report.
#[test]
fn a_uuidless_report_is_admitted_when_no_card_is_adoptable() {
    let inventory = GpuInventory::known(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
    let ledger = VramLedger::new(&inventory, no_margin().into(), None);
    ledger.install_probe_stub(None);
    let handle = uuidless_report(32_607);
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("the single-GPU fallback is unchanged with nothing hidden");
    assert_eq!(ledger.health()[0].workers.len(), 1);
}

/// Everything downstream of an adoption: the ledger row, and the inventory,
/// which then answers for the card in the device-key resolver, `/metadata`
/// and `/health`'s `gpus[]`.
#[test]
fn an_adopted_row_reaches_the_ledger_and_the_inventory() {
    let profiles = Arc::new(FakeProfiles::default());
    let inventory = GpuInventory::masked(vec![nvidia(1, "GPU-3c4d", "TEST 9001", 100_000)]);
    // Before the load the host is unknown on both sides.
    assert_eq!(inventory.default_gpu_name(), None);
    assert_eq!(inventory.resolve_device_key(None), None);
    let ledger = VramLedger::new(
        &inventory,
        VramBudget::default().into(),
        Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
    );
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: "GPU-3c4d".to_owned(),
        total_mb: 100_000,
        free_mb: 40_000,
    }]));
    let handle = loaded_on("GPU-3c4d", Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .expect("adopted");
    push_memory(&handle, 40_000, 0);
    ledger.ingest_all_for_test();
    let health = ledger.health();
    assert_eq!(health.len(), 1, "the adopted row is the vram[] row");
    let gpu = &health[0];
    assert_eq!(gpu.gpu_uuid, "GPU-3c4d");
    assert_eq!(gpu.gpu_name, "TEST 9001", "the row's own name");
    assert_eq!(gpu.total_mb, 100_000, "the row's own total");
    assert_eq!(gpu.gpu_arch.as_deref(), Some(ARCH), "the store key");
    assert!(
        gpu.external_known,
        "external readings reach the adopted row"
    );
    assert_eq!(gpu.external_mb, 100_000 - 40_000 - 1000);
    assert_eq!(
        gpu.reserve_rule, "capped_default",
        "the reserve rule applies as on any other row"
    );
    assert!(gpu.limit_mb > 0 && gpu.headroom_mb > 0);
    assert_eq!(ledger.gpu_arch("GPU-3c4d").as_deref(), Some(ARCH));
    // The store row is written under that arch and that card's name.
    measured_window(&handle, &admission, 8);
    let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(written.arch, ARCH);
    assert_eq!(written.gpu_name, "TEST 9001");
    // The inventory side, on the clone the manager holds. The calibration
    // overlay is omitted unless the *name* answers.
    let mut published = inventory.priced_gpus().expect("gpus[] lists the card");
    publish_adopted_totals(&mut published, &health);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].uuid, "GPU-3c4d");
    assert_eq!(published[0].total_mb, 100_000);
    assert_eq!(
        inventory.resolve_device_key(None).as_deref(),
        Some("GPU-3c4d")
    );
    assert_eq!(
        inventory.resolve_device_key(Some("1")).as_deref(),
        Some("GPU-3c4d"),
        "and by the index the operator's mask is written in"
    );
    assert_eq!(inventory.default_gpu_name().as_deref(), Some("TEST 9001"));
    assert_eq!(inventory.default_gpu_arch().as_deref(), Some(ARCH));
    assert!(
        inventory.gpus().is_none(),
        "the mask still hides whatever no worker reported"
    );
}

/// Two GPUs of the same model and size cannot be told apart by any memory
/// cross-check, so they decide what a mis-ordered enumeration does.
#[test]
fn a_swapped_enumeration_admits_under_the_gpu_the_worker_is_on() {
    let ledger = rocm_ledger();
    // Pinned to (and believed on) GPU A; came up on GPU B.
    let handle = loaded_rocm(Some("0000:0c:00.0"), Some(24_576));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, Some(AMD_A))
        .expect("admitted despite the divergence");
    assert_eq!(
        admitted_gpu(&ledger, 0),
        (AMD_B.to_owned(), "g/a".to_owned()),
        "charged to the GPU it is on, not the one the pin named"
    );

    // The alarm itself.
    let report = rocm_report(Some("0000:0c:00.0"), Some(24_576));
    let state = ledger.lock();
    let diverged = VramLedger::resolve_gpu(&state, &report, Some(AMD_A));
    assert_eq!(
        diverged.admit.map(|(key, _)| key),
        Some(AMD_B.to_owned()),
        "still admitted, under the resolved GPU"
    );
    assert!(
        matches!(diverged.log, Some(GpuLog::PinDiverged { .. })),
        "and the mis-order is what gets logged"
    );
    // The same registration whose pin agrees says nothing at all.
    let agreed = VramLedger::resolve_gpu(&state, &report, Some(AMD_B));
    assert!(agreed.log.is_none(), "no alarm when the two agree");
    // Nor when the caller has no belief to compare against.
    assert!(VramLedger::resolve_gpu(&state, &report, None).log.is_none());
}

/// The cross-check's exact edges, in both halves of `max(5%, 512 MB)`.
#[test]
fn the_total_tolerance_is_five_percent_with_a_512mb_floor() {
    // 24 GB: 5% is 1228 MB, the wider of the two.
    let big = |total: u64| {
        rocm_ledger()
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:03:00.0"), Some(total)),
                None,
            )
            .is_some()
    };
    assert!(big(24_576 - 1228), "a difference of exactly the tolerance");
    assert!(!big(24_576 - 1229), "and one MB past it");
    assert!(
        big(24_576 + 1228),
        "symmetric: the worker may read high too"
    );
    assert!(!big(24_576 + 1229));

    // 8 GB: 5% is 409 MB, so the absolute floor is what decides.
    let small = |total: u64| {
        VramLedger::for_test_gpus(
            &[(AMD_A, "AMD gfx1030 (8 GB)", 8192, Some("0000:03:00.0"))],
            VramBudget::default(),
            None,
        )
        .register_worker(
            "g/a",
            item_cost(4),
            &loaded_rocm(Some("0000:03:00.0"), Some(total)),
            None,
        )
        .is_some()
    };
    assert!(
        small(8192 - 512),
        "the 512 MB floor admits where 5% would not"
    );
    assert!(!small(8192 - 513), "and stops one MB later");
}

/// A UUID match carries **no** memory check, deliberately.
#[test]
fn a_uuid_match_admits_whatever_the_totals_say() {
    let ledger = ledger(24_576, VramBudget::default());
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1000),
        gpu_uuid: Some(GPU.to_owned()),
        // A number that no tolerance would ever admit.
        gpu_total_mb: Some(1),
        ..LoadReport::default()
    }));
    let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted on the UUID alone");
    assert_eq!(admitted_gpu(&ledger, 0).0, GPU);
}

/// The single-GPU fallback requires the UUID to be **absent** (as on every
/// ROCm worker), not merely unmatched.
#[test]
fn a_present_but_unmatched_uuid_refuses_the_single_gpu_fallback() {
    let bare = |uuid: Option<&str>| {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            gpu_uuid: uuid.map(str::to_owned),
            // Exactly the GPU's own total, so only the UUID decides.
            gpu_total_mb: Some(24_576),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry)) as TelemetryHandle
    };
    let single = ledger(24_576, VramBudget::default());
    assert!(
        single
            .register_worker("g/a", item_cost(4), &bare(Some("MIG-somewhere")), None)
            .is_none(),
        "a reported identity that matches nothing is not this GPU"
    );
    let _admission = single
        .register_worker("g/a", item_cost(4), &bare(None), None)
        .expect("the same report with no identity claim does fall back");
    assert_eq!(admitted_gpu(&single, 0).0, GPU);
}
