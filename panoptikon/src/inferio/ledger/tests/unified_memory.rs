//! Unified-memory devices: MPS, AMD APUs and CPU-only hosts.
use super::*;

/// The unified-memory total is adopted from the first worker, unmoved by an
/// agreeing report, re-adopted when the wired limit moves, and refused
/// outside `0 < reported <= host RAM`.
#[test]
fn a_unified_devices_total_is_adopted_re_adopted_and_sanity_bounded() {
    let seed = MAC_RAM_MB / 4 * 3;
    let raised = MAC_RAM_MB / 10 * 9;
    // (label, the loads in order as (reported total, admits), total in force)
    for (label, loads, expected) in [
        (
            "the figure allocations are actually judged against wins",
            vec![(Some(raised), true)],
            raised,
        ),
        (
            "zero is not a total, and the seed is what keeps budgets defined",
            vec![(Some(0), false)],
            seed,
        ),
        (
            "more than the machine has is not this GPU's budget either",
            vec![(Some(MAC_RAM_MB + 1), false)],
            seed,
        ),
        (
            "a report with no MPS facts at all — no torch, a remote-API \
             impl — registers nothing and adopts nothing",
            vec![(None, false)],
            seed,
        ),
        (
            "a second report inside the cross-check tolerance is admitted \
             and is not a second opinion to average in",
            vec![(Some(raised), true), (Some(raised - 100), true)],
            raised,
        ),
        (
            "a raised wired limit lands far outside that tolerance, and \
             re-adopts rather than refusing every replica until a restart",
            vec![(Some(seed), true), (Some(raised), true)],
            raised,
        ),
        (
            "the sanity bound still holds after adoption, and the total in \
             force is untouched",
            vec![(Some(raised), true), (Some(MAC_RAM_MB + 1), false)],
            raised,
        ),
    ] {
        let ledger = mps_ledger();
        assert_eq!(gpu_total_mb(&ledger), seed, "the probe's seed");
        let mut admitted = vec![];
        for (index, (reported, admits)) in loads.into_iter().enumerate() {
            let handle = loaded_mps(reported);
            let admission =
                ledger.register_worker(&format!("g/{index}"), item_cost(4), &handle, None);
            assert_eq!(admission.is_some(), admits, "{label}");
            admitted.extend(admission);
        }
        assert_eq!(gpu_total_mb(&ledger), expected, "{label}");
        if !admitted.is_empty() {
            assert_eq!(admitted_gpu(&ledger, 0).0, MPS_GPU, "{label}");
        }
    }
}

/// Push a memory sample whose pool and live figures differ, as Metal's
/// allocator reports them.
fn push_pool(
    handle: &TelemetryHandle,
    free_mb: u64,
    reserved_mb: u64,
    allocated_mb: u64,
    source: &str,
) {
    let mut telemetry = handle.lock().unwrap();
    telemetry.memory = Some(Timestamped::now(MemorySample {
        free_mb: Some(free_mb),
        total_mb: None,
        free_source: Some(source.to_owned()),
        reserved_mb: Some(reserved_mb),
        allocated_mb: Some(allocated_mb),
        ..MemorySample::default()
    }));
}

/// Freeing MPS tensors into the pool does not move `available`: the host
/// wires a Metal pool's cached blocks as a driver holds a `cudaMalloc`'d
/// one, so both allocators net the **pool**, not the live bytes.
#[test]
fn both_allocators_net_the_pool_against_their_own_free_reading() {
    const TOTAL: u64 = 110_100;
    const HOG: u64 = 89_600;
    const BASE: u64 = 1_000;
    let mps = mps_ledger();
    let handle = loaded_mps(Some(TOTAL));
    let admission = mps
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    let mut externals = Vec::new();
    for live in [0u64, 3_000, 6_000, 9_000, 12_000] {
        // The pool at the learned Metal ratio, wired into the RAM left.
        let pool = (live as f64 * 2.9) as u64;
        push_ram(&handle, TOTAL, MAC_RAM_MB - HOG - BASE - pool, pool, live);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        externals.push(mps.health()[0].external_mb);
    }
    assert!(
        externals.iter().all(|external| *external == HOG),
        "the hog held {HOG} MiB throughout and let none of it go; \
         netting the live figure instead books our own cache to it and \
         this reads 89 600, 95 300, 101 000, 106 700, 112 400: \
         {externals:?}"
    );

    // The pool held flat while its live tensors are freed into it:
    // `available` does not move, so neither may `external`.
    let mut externals = Vec::new();
    for live in [24_576u64, 12_288, 0] {
        push_ram(
            &handle,
            TOTAL,
            MAC_RAM_MB - HOG - BASE - 24_584,
            24_584,
            live,
        );
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        externals.push(mps.health()[0].external_mb);
    }
    assert_eq!(
        externals,
        vec![HOG; 3],
        "freeing a tensor into the pool returns the host nothing"
    );

    // On a `cudaMalloc`'d pool NVML has already lost every cached block, so
    // reading live bytes would invent headroom.
    let cuda = ledger(TOTAL, no_margin());
    let handle = loaded(Some(BASE), Some(0));
    let admission = cuda
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    let mut externals = Vec::new();
    for live in [0u64, 1_000, 2_000, 3_000, 4_000] {
        let pool = (live as f64 * 2.9) as u64;
        push_pool(&handle, TOTAL - HOG - BASE - pool, pool, live, "nvml");
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        externals.push(cuda.health()[0].external_mb);
    }
    assert!(
        externals.iter().all(|external| *external == HOG),
        "a driver pool is memory the card has really handed out: \
         {externals:?}"
    );
}

/// On MPS `total` is `recommended_max_memory()` while `free` is `available`
/// clipped to it, so `total - free` under-reads a loaded machine by the
/// difference (20 972 MiB on a 128 GiB Mac). Summed in the RAM domain, it
/// reads the whole hold.
#[test]
fn external_usage_on_a_unified_device_is_measured_in_the_ram_domain() {
    const TOTAL: u64 = 110_100;
    const HOG: u64 = 89_600;
    const BASE: u64 = 1_000;
    let mps = mps_ledger();
    let handle = loaded_mps(Some(TOTAL));
    let admission = mps
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    let mut externals = Vec::new();
    for live in [0u64, 3_000, 6_000, 9_000, 12_000] {
        // The RAM left with the hog, our base and our whole pool wired in it.
        let pool = (live as f64 * 2.9) as u64;
        let available = MAC_RAM_MB - HOG - BASE - pool;
        push_ram(&handle, TOTAL, available, pool, live);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        externals.push(mps.health()[0].external_mb);
    }
    assert!(
        externals.iter().all(|external| *external == HOG),
        "the hog holds {HOG} MiB at every sample; in the device's own \
         currency this reads 68 628, 89 % of the hold: {externals:?}"
    );

    // A 30 000 MiB pool over 12 000 of live tensors: our own cache is not
    // somebody else's.
    push_ram(
        &handle,
        TOTAL,
        MAC_RAM_MB - 60_000 - BASE - 30_000,
        30_000,
        12_000,
    );
    admission
        .request_grant(1, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    assert_eq!(mps.health()[0].external_mb, 60_000, "the hog, and only it");

    // A worker too old to state its RAM basis is priced over the clipped
    // reading.
    let stale = mps_ledger();
    let handle = loaded_mps(Some(TOTAL));
    let admission = stale
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_pool(&handle, (MAC_RAM_MB - HOG - BASE).min(TOTAL), 0, 0, "mps");
    admission
        .request_grant(1, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        stale.health()[0].external_mb,
        TOTAL - (MAC_RAM_MB - HOG - BASE) - BASE,
        "no basis, no RAM-domain sum: today's arithmetic stands"
    );
}

/// The resident is charged the pool the batch left behind, not the sampled
/// high-water, which would make `external_mb` decay to 0 under a hog as our
/// own peak grew.
#[test]
fn a_resident_is_charged_the_pool_it_holds_not_the_peak_it_touched() {
    const TOTAL: u64 = 122_880;
    const HOG: u64 = 99_968;
    let mps = mps_ledger();
    let handle = loaded_mps(Some(TOTAL));
    let admission = mps
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    let available = MAC_RAM_MB - HOG - 1_000 - 100;
    let mut externals = Vec::new();
    for peak in [4_000u64, 12_000, 20_000] {
        let token = admission.request_grant(4, None, 1, 0).expect("granted");
        let mut batch = measurement_with_free(4, 100, peak, available, "mps");
        // The in-batch maximum grows every window; the pool left is 100 MiB.
        batch.reserved_after_mb = Some(100);
        batch.ram_total_mb = Some(MAC_RAM_MB);
        batch.ram_available_mb = Some(available);
        handle.lock().unwrap().record_measurements(vec![batch]);
        token.finish(WindowOutcome::Responded { oom: None });
        externals.push(mps.health()[0].external_mb);
    }
    assert_eq!(
        externals,
        vec![HOG; 3],
        "the hog let nothing go; charged the peak this decays away under it"
    );
}

/// Before any worker has loaded, the ledger holds the probe's 75 % seed as
/// its total; priced in the RAM domain, a hog still leaves the room the
/// machine actually has rather than `limit_mb = 0`.
#[tokio::test]
async fn a_mac_that_has_not_adopted_its_total_yet_prices_the_ram_it_has() {
    const SEED: u64 = MAC_RAM_MB / 4 * 3;
    const HOG: u64 = 98_000;
    let ledger = mps_ledger();
    // `MemoryQuery::Mps` reports physical RAM as the total and `available`
    // as the free reading, so this path needs no worker to answer.
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: MPS_GPU.to_owned(),
        total_mb: MAC_RAM_MB,
        free_mb: MAC_RAM_MB - HOG,
    }]));
    let (_reservation, exceeds_headroom) = ledger
        .reserve_load_signalling_for_test("g/a", item_cost(4), MPS_GPU, None)
        .await
        .expect("a known GPU charges the load, headroom or none");
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.total_mb, SEED, "the seed, not yet superseded");
    assert_eq!(gpu.external_mb, HOG, "and the hog, not the seed clipped");
    assert_eq!(
        gpu.limit_mb,
        MAC_RAM_MB - HOG - gpu.reserve_mb,
        "against the 0 the clipped term published for three seconds"
    );
    assert!(
        !exceeds_headroom,
        "30 GiB of room prices this load without a warning"
    );

    // A machine with nothing left still admits the load, clamped to the
    // headroom.
    let full = mps_ledger();
    full.install_probe_stub(Some(vec![GpuMemory {
        uuid: MPS_GPU.to_owned(),
        total_mb: MAC_RAM_MB,
        free_mb: 0,
    }]));
    let (_reservation, exceeds_headroom) = full
        .reserve_load_signalling_for_test("g/a", item_cost(4), MPS_GPU, None)
        .await
        .expect("still a reservation, never a refusal");
    assert_eq!(full.health()[0].limit_mb, 0);
    assert_eq!(full.health()[0].load_reservations_mb, 0, "clamped to it");
    assert!(exceeds_headroom, "and the operator is told, not refused");
}

/// A per-batch frame states its RAM basis too, so it prices the same instant
/// as the response-level sample does rather than 8 192 MiB apart
/// (`hw.memsize - recommended_max_memory()`) down the no-basis fallback.
#[test]
fn a_per_batch_frame_prices_the_ram_domain_as_the_response_sample_does() {
    const TOTAL: u64 = 122_880;
    // Our base plus the batch's 40 MiB pool.
    const OURS: u64 = 1_040;
    const AVAILABLE: u64 = MAC_RAM_MB - 113_536 - OURS;
    let priced = |basis: bool| {
        let mps = mps_ledger();
        let handle = loaded_mps(Some(TOTAL));
        let admission = mps
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        let token = admission.request_grant(4, None, 1, 0).expect("granted");
        let mut batch = measurement_with_free(4, 0, 40, AVAILABLE.min(TOTAL), "mps");
        if basis {
            batch.ram_total_mb = Some(MAC_RAM_MB);
            batch.ram_available_mb = Some(AVAILABLE);
        }
        // No response-level sample: only the per-batch frame.
        handle.lock().unwrap().record_measurements(vec![batch]);
        token.finish(WindowOutcome::Responded { oom: None });
        mps.health()[0].external_mb
    };
    // What the response-level sample prices the same instant at.
    let mps = mps_ledger();
    let handle = loaded_mps(Some(TOTAL));
    let admission = mps
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_ram(&handle, TOTAL, AVAILABLE, 40, 40);
    admission
        .request_grant(1, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    let response_level = mps.health()[0].external_mb;

    assert_eq!(
        (priced(true), response_level),
        (113_536, 113_536),
        "one domain, whichever frame carried the reading"
    );
    assert_eq!(
        response_level - priced(false),
        MAC_RAM_MB - TOTAL,
        "and the fallback a worker too old to state a basis takes is the \
         8 192 MiB step this pins away"
    );
}

/// `/health` publishes the adopted total in the `gpus` inventory, not the
/// probe's seed beside it.
#[test]
fn the_published_inventory_carries_the_adopted_total() {
    let seed = MAC_RAM_MB / 4 * 3;
    let raised = MAC_RAM_MB / 10 * 9;
    let ledger = mps_ledger();
    let mut gpus = vec![crate::inferio::gpu::GpuInfo {
        index: 0,
        uuid: MPS_GPU.to_owned(),
        name: "Apple M3 Max (128 GB)".to_owned(),
        total_mb: seed,
        compute_cap: None,
        bdf: None,
        gfx_target_version: None,
        unified_ram_mb: Some(MAC_RAM_MB),
        vram_carveout_mb: None,
    }];
    publish_adopted_totals(&mut gpus, &ledger.health());
    assert_eq!(gpus[0].total_mb, seed, "before any load, the seed stands");

    let handle = loaded_mps(Some(raised));
    assert!(
        ledger
            .register_worker("g/0", item_cost(4), &handle, None)
            .is_some()
    );
    publish_adopted_totals(&mut gpus, &ledger.health());
    assert_eq!(gpus[0].total_mb, raised, "one device, one total");
    assert_eq!(
        gpu_total_mb(&ledger),
        raised,
        "the same figure admission uses"
    );

    // A device the ledger does not know keeps whatever the probe said.
    gpus[0].uuid = "GPU-OTHER".to_owned();
    gpus[0].total_mb = seed;
    publish_adopted_totals(&mut gpus, &ledger.health());
    assert_eq!(gpus[0].total_mb, seed);
}

/// The BIOS UMA carve-out amdgpu publishes as an APU's whole VRAM total.
const APU_CARVEOUT_MB: u64 = 512;
/// Carve-out + GTT: what admission actually budgets against.
const APU_TOTAL_MB: u64 = APU_CARVEOUT_MB + 64 * 1024;

/// An APU row as `rocm.rs` builds one, at `0000:03:00.0`.
fn apu_device(index: u32) -> crate::inferio::gpu::GpuInfo {
    crate::inferio::gpu::GpuInfo {
        index,
        uuid: AMD_A.to_owned(),
        name: "AMD gfx1151 APU (128 GB)".to_owned(),
        total_mb: APU_TOTAL_MB,
        compute_cap: None,
        bdf: Some("0000:03:00.0".to_owned()),
        gfx_target_version: Some(110_501),
        unified_ram_mb: Some(128 * 1024),
        vram_carveout_mb: Some(APU_CARVEOUT_MB),
    }
}

fn apu_ledger(gpus: Vec<crate::inferio::gpu::GpuInfo>) -> Arc<VramLedger> {
    VramLedger::new(
        &GpuInventory::known_rocm(gpus),
        VramBudget::default().into(),
        None,
    )
}

/// The either-of cross-check.
#[test]
fn an_apu_replica_is_admitted_on_either_total() {
    // Two GPUs, so the cross-check gates a BDF match, not the single-GPU
    // fallback.
    let dgpu = crate::inferio::gpu::GpuInfo {
        index: 1,
        uuid: AMD_B.to_owned(),
        name: "AMD gfx1100 (24 GB)".to_owned(),
        total_mb: 24_576,
        compute_cap: None,
        bdf: Some("0000:0c:00.0".to_owned()),
        gfx_target_version: Some(110_000),
        unified_ram_mb: None,
        vram_carveout_mb: None,
    };
    for reported in [APU_CARVEOUT_MB, APU_TOTAL_MB] {
        let ledger = apu_ledger(vec![apu_device(0), dgpu.clone()]);
        let handle = loaded_rocm(Some("0000:03:00.0"), Some(reported));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap_or_else(|| panic!("a HIP total of {reported} MiB must admit"));
        assert_eq!(admitted_gpu(&ledger, 0).0, AMD_A);
        let gpu = ledger
            .health()
            .into_iter()
            .find(|gpu| gpu.gpu_uuid == AMD_A)
            .expect("the APU");
        assert_eq!(
            gpu.total_mb, APU_TOTAL_MB,
            "and the budget is the ledger's own figure either way — the \
             report identifies the GPU, it does not re-price it"
        );
    }
    // A figure that is neither is still a refusal.
    let ledger = apu_ledger(vec![apu_device(0), dgpu.clone()]);
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:03:00.0"), Some(8192)),
                None
            )
            .is_none(),
        "8 GB is neither the carve-out nor the unified total"
    );
    // An absent total fails as everywhere else.
    let ledger = apu_ledger(vec![apu_device(0), dgpu]);
    assert!(
        ledger
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:03:00.0"), None),
                None
            )
            .is_none()
    );
}

/// The cross-check's window, at both edges and on both candidates.
#[test]
fn the_either_of_window_is_bounded_at_both_candidates() {
    let admits = |reported: u64| {
        apu_ledger(vec![apu_device(0)])
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:03:00.0"), Some(reported)),
                None,
            )
            .is_some()
    };
    // The carve-out candidate: 512 MB, so the window is ±128 MB, not ±512.
    assert_eq!(total_tolerance_mb(APU_CARVEOUT_MB), 128);
    assert!(admits(APU_CARVEOUT_MB + 128));
    assert!(admits(APU_CARVEOUT_MB - 128));
    assert!(!admits(APU_CARVEOUT_MB + 129));
    assert!(!admits(APU_CARVEOUT_MB - 129));
    // The unified-total candidate: 5% of 66048 MB.
    let tolerance = total_tolerance_mb(APU_TOTAL_MB);
    assert_eq!(tolerance, APU_TOTAL_MB / 20);
    assert!(admits(APU_TOTAL_MB + tolerance));
    assert!(!admits(APU_TOTAL_MB + tolerance + 1));
    assert!(!admits(0), "zero is not a GPU");
    // Unchanged at dGPU scale: 5% above 10 GB, 512 MB between 2 and 10 GB.
    assert_eq!(total_tolerance_mb(24_576), 1228);
    assert_eq!(total_tolerance_mb(8192), 512);
    assert_eq!(total_tolerance_mb(2048), 512);
}

/// A free sample whose **own total** does not describe the GPU it was
/// admitted under is dropped: `external = total - free - ours` would turn
/// the currency difference into headroom.
#[test]
fn a_free_sample_whose_total_names_another_gpu_is_dropped() {
    let ledger = apu_ledger(vec![apu_device(0)]);
    let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    assert!(!ledger.health()[0].external_known, "no reading yet");

    // 24 GB free of a 24 GB GPU, on a GPU the ledger knows as 64.5 GB.
    push_memory_with_total(&handle, 24_000, 0, Some(24_576), "amdgpu-sysfs");
    ledger.ingest_all_for_test();
    assert!(
        !ledger.health()[0].external_known,
        "the sample is discarded, not averaged in"
    );

    assert_eq!(
        ledger.lock().free_total_mismatch_logged.len(),
        1,
        "and it said so once"
    );

    // The same worker reporting this GPU's own currency lands.
    push_memory_with_total(&handle, 60_000, 0, Some(APU_TOTAL_MB), "amdgpu-sysfs");
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert!(gpu.external_known);
    assert_eq!(gpu.external_mb, APU_TOTAL_MB - 60_000 - 1000);
    // Agreement clears the once-per-replica log guard, so a later genuine
    // mismatch after a re-adoption is reported.
    assert!(ledger.lock().free_total_mismatch_logged.is_empty());
}

/// The guard is a no-op for well-behaved workers on CUDA, MPS and a flagged
/// APU.
#[test]
fn well_behaved_samples_still_land_on_every_backend() {
    // CUDA.
    let cuda = ledger(32_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let _admission = cuda
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory_with_total(&handle, 20_000, 0, Some(32_000), "nvml");
    cuda.ingest_all_for_test();
    assert_eq!(cuda.health()[0].external_mb, 32_000 - 20_000 - 1000);

    // MPS: adoption runs before the sample that rides the load report.
    let mps = mps_ledger();
    let raised = MAC_RAM_MB / 10 * 9;
    let handle = loaded_mps(Some(raised));
    {
        let mut telemetry = handle.lock().unwrap();
        let load = telemetry.load.as_mut().expect("the load report");
        load.value.memory = Some(MemorySample {
            free_mb: Some(raised / 2),
            total_mb: Some(raised),
            free_source: Some("mps".to_owned()),
            reserved_mb: Some(0),
            allocated_mb: Some(0),
            ..MemorySample::default()
        });
    }
    let _admission = mps
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    let gpu = &mps.health()[0];
    assert!(
        gpu.external_known,
        "the load-report sample landed against the adopted total"
    );
    assert_eq!(gpu.external_mb, raised - raised / 2 - 1000);

    // A flagged APU worker: carve+GTT on both sides.
    let apu = apu_ledger(vec![apu_device(0)]);
    let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
    let _admission = apu
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory_with_total(&handle, 60_000, 0, Some(APU_TOTAL_MB), "amdgpu-sysfs");
    apu.ingest_all_for_test();
    let gpu = &apu.health()[0];
    assert!(gpu.external_known);
    assert_eq!(gpu.external_mb, APU_TOTAL_MB - 60_000 - 1000);
}

/// Total adoption is an **MPS** mechanism and must not touch an APU.
#[test]
fn an_apus_total_is_never_adopted_from_a_worker() {
    let ledger = apu_ledger(vec![apu_device(0)]);
    // One GPU and a report with neither a UUID nor an address: the shape
    // that would adopt on MPS.
    let handle = loaded_rocm(None, Some(APU_CARVEOUT_MB));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("the single-GPU fallback still admits it");
    assert_eq!(
        ledger.health()[0].total_mb,
        APU_TOTAL_MB,
        "the carve-out must not become this GPU's budget"
    );
}

/// The halving is **runtime-only**: a stored anchor is a claim about a batch
/// size this machine once ran, and no death unmeasures one.
#[test]
fn a_deaths_halved_anchor_never_reaches_the_store() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = VramLedger::for_test_gpus(
        &[(MPS_GPU, "Apple M3 Max (128 GB)", MAC_RAM_MB / 4 * 3, None)],
        no_margin(),
        Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
    );
    {
        let mut state = ledger.lock();
        let gpu = state.gpus.get_mut(MPS_GPU).expect("the GPU");
        gpu.unified_ram_mb = Some(MAC_RAM_MB);
        // No probe on a Mac names the architecture; the load report does.
        gpu.arch = None;
    }

    // The MPS load report a store write needs.
    let handle = {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("mps".to_owned()),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_name: Some("Apple M3 Max (128 GB)".to_owned()),
            gpu_arch: Some("apple-m3".to_owned()),
            gpu_total_mb: Some(MAC_RAM_MB / 4 * 3),
            torch_version: Some("2.7.1".to_owned()),
            dtype: Some("fp32".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry)) as TelemetryHandle
    };
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory(&handle, 60_000, 0);
    for units in [4, 8, 16] {
        measured_window(&handle, &admission, units);
    }
    let written_row = profiles.updates.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        written_row.max_units_measured, 16,
        "the measured anchor is what was written"
    );
    assert_eq!(
        written_row.arch, "apple-m3",
        "and it is keyed by the architecture the load report named"
    );
    let written = profiles.updates.lock().unwrap().len();

    admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::WorkerDied);
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        8,
        "the live anchor is halved: a worker death is a negative sample"
    );

    // A window that moves the fit but not the anchor, so a write happens.
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(2, 0, 140)]);
    token.finish(WindowOutcome::Responded { oom: None });

    let updates = profiles.updates.lock().unwrap();
    assert!(
        updates.len() > written,
        "the refit really did produce a write, or this proves nothing"
    );
    assert!(
        updates[written..]
            .iter()
            .all(|update| update.max_units_measured >= 16),
        "no write after the death may lower the persisted anchor: {:?}",
        updates
            .iter()
            .map(|update| update.max_units_measured)
            .collect::<Vec<_>>()
    );
}

/// Halving bottoms out at **one unit**: zero is the "no local measurement"
/// sentinel, which turns the ratchet ceiling off.
#[test]
fn repeated_deaths_never_take_the_anchor_below_one() {
    let ledger = mps_ledger();
    let handle = loaded_mps(Some(MAC_RAM_MB / 4 * 3));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory(&handle, 60_000, 0);
    measured_window(&handle, &admission, 2);
    assert_eq!(ledger.health()[0].workers[0].max_units_measured, 2);
    for _ in 0..3 {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::WorkerDied);
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            1,
            "2 → 1, and 1 → 1: the ratchet ceiling stays on"
        );
    }

    let fresh = mps_ledger();
    let handle = loaded_mps(Some(MAC_RAM_MB / 4 * 3));
    let admission = fresh
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory(&handle, 60_000, 0);
    admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::WorkerDied);
    assert_eq!(
        fresh.health()[0].workers[0].max_units_measured,
        0,
        "nothing was measured, so there is no anchor to halve"
    );
}

/// The pool-margin ceiling is the **allocator's**, not the host's: Metal
/// keeps 2.3-2.9x the allocated peak in its pool, which CUDA's ceiling of
/// 2.0 would under-price.
#[test]
fn metals_pool_ratio_is_learned_whole_where_cudas_ceiling_would_cut_it() {
    // 100 MiB of allocation per unit, 260 MiB of pool: ratio 2.6.
    let grew = |units: u64| BatchMeasurement {
        reserved_before_mb: Some(0),
        peak_reserved_mb: Some(260 * units),
        allocated_before_mb: Some(0),
        peak_allocated_mb: Some(100 * units),
        ..measurement(units, 0, 0)
    };
    let margin_of = |ledger: &Arc<VramLedger>| {
        ledger.health()[0].workers[0]
            .fit
            .as_ref()
            .expect("a fit")
            .pool_margin
    };

    let mps = mps_ledger();
    let handle = loaded_mps(Some(MAC_RAM_MB / 4 * 3));
    let admission = mps
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory(&handle, 90_000, 0);
    for units in [1u64, 2, 4] {
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![grew(units)]);
        clean_window(&admission);
    }
    assert!((margin_of(&mps) - 2.6).abs() < 1e-9, "{}", margin_of(&mps));

    // The grant covers the pool the batch would take.
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let grant = *token.grant();
    assert!(
        grant.mb >= 260 * grant.unit_budget,
        "{} MiB for {} units",
        grant.mb,
        grant.unit_budget
    );

    // The same measurements on a CUDA host stop at CUDA's ceiling.
    let cuda = ledger(1_000_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = cuda
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    push_memory(&handle, 900_000, 0);
    for units in [1u64, 2, 4] {
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![grew(units)]);
        clean_window(&admission);
    }
    assert!(
        (margin_of(&cuda) - POOL_MARGIN_MAX_CUDA).abs() < 1e-9,
        "{}",
        margin_of(&cuda)
    );

    // The CPU device of the same Mac also stops there: the ceiling is per
    // device, because that device's allocator is the process heap.
    let pair = VramLedger::new(
        &GpuInventory::known_mps(MAC_RAM_MB),
        no_margin().into(),
        None,
    );
    pair.install_probe_stub(None);
    let cpu_handle = loaded_on_cpu(Some(MAC_RAM_MB));
    let on_ram = pair
        .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
        .expect("admitted on RAM");
    push_memory_with_total(&cpu_handle, MAC_RAM_MB / 2, 0, Some(MAC_RAM_MB), "ram");
    for units in [1u64, 2, 4] {
        cpu_handle
            .lock()
            .unwrap()
            .record_measurements(vec![grew(units)]);
        clean_window(&on_ram);
    }
    let on_heap = pair
        .health()
        .into_iter()
        .find(|gpu| gpu.gpu_uuid == cpu::DEVICE_KEY)
        .expect("the CPU device")
        .workers
        .swap_remove(0)
        .fit
        .expect("a fit")
        .pool_margin;
    assert!((on_heap - POOL_MARGIN_MAX_CUDA).abs() < 1e-9, "{on_heap}");
}

/// The CPU device ships with the cap off, like every other device: with no
/// external usage its limit is RAM less its reserve.
#[test]
fn the_cpu_device_ships_without_a_ceiling() {
    let cpu = cpu_ledger(no_margin());
    let gpu = &cpu.health()[0];
    assert_eq!(gpu.gpu_uuid, "CPU");
    assert_eq!(gpu.gpu_name, "CPU (64 GB)");
    assert_eq!(gpu.total_mb, CPU_RAM_MB, "the total is RAM itself");
    assert_eq!(gpu.cap_fraction, None);
    assert_eq!(gpu.reserve_mb, cpu::ram_reserve_mb(CPU_RAM_MB));
    assert_eq!(gpu.limit_mb, CPU_RAM_MB - gpu.reserve_mb);
}

/// A configured cap applies to the CPU device, from the per-GPU override or
/// the section-wide one alike.
#[test]
fn a_configured_ceiling_overrides_the_cpu_default() {
    let per_gpu = cpu_ledger(
        VramBudgets::uniform(VramBudget {
            margin: Some(0.0),
            cap_fraction: None,
            knee_max_bucket_dispersion: None,
        })
        .with_gpu(
            "CPU",
            VramBudget {
                margin: Some(0.0),
                cap_fraction: Some(0.5),
                knee_max_bucket_dispersion: None,
            },
        ),
    );
    assert_eq!(per_gpu.health()[0].cap_fraction, Some(0.5));
    assert_eq!(per_gpu.health()[0].limit_mb, CPU_RAM_MB / 2);

    let section_wide = cpu_ledger(VramBudget {
        margin: Some(0.0),
        cap_fraction: Some(1.0),
        knee_max_bucket_dispersion: None,
    });
    assert_eq!(section_wide.health()[0].cap_fraction, Some(1.0));
    assert_eq!(
        section_wide.health()[0].limit_mb,
        CPU_RAM_MB - CPU_RAM_MB / 10,
        "the whole machine but its RAM reserve, which no setting lowers"
    );
}

/// On a CPU host the join is the single-GPU fallback, cross-checked against
/// physical RAM, which `psutil` reports from the same sources the host reads.
#[test]
fn a_cpu_worker_registers_against_the_ram_gpu() {
    let ledger = cpu_ledger(no_margin());
    let handle = loaded_cpu(Some(CPU_RAM_MB));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted under the only GPU there is");
    assert_eq!(
        admitted_gpu(&ledger, 0),
        ("CPU".to_owned(), "g/a".to_owned())
    );

    // A report describing some *other* machine's memory is refused.
    let foreign = cpu_ledger(no_margin());
    assert!(
        foreign
            .register_worker("g/a", item_cost(4), &loaded_cpu(Some(8192)), None)
            .is_none(),
        "8 GB is not this 64 GB machine"
    );
}

/// On a mixed host (two CUDA GPUs and the CPU device) each replica is
/// admitted against the device **its own report** names, and both are
/// priced and ramp.
#[test]
fn a_cpu_replica_is_priced_beside_the_gpus_of_a_cuda_host() {
    let inventory = GpuInventory::known(vec![
        nvidia(0, "GPU-1a2b", "TEST 9000", 32_607),
        nvidia(1, "GPU-3c4d", "TEST 9001", 100_000),
    ])
    .with_cpu(CPU_RAM_MB, crate::inferio::cpu::MemRoots::default());
    let ledger = VramLedger::new(&inventory, no_margin().into(), None);
    ledger.install_probe_stub(None);

    // The pin said GPU, the report says CPU: the report wins.
    let cpu_handle = loaded_on_cpu(Some(CPU_RAM_MB));
    let cpu_admission = ledger
        .register_worker("g/cpu", item_cost(4), &cpu_handle, Some("GPU-1a2b"))
        .expect("admitted on the CPU device");
    let gpu_handle = loaded_on("GPU-3c4d", Some(1000), Some(0));
    let gpu_admission = ledger
        .register_worker("g/gpu", item_cost(4), &gpu_handle, Some("GPU-3c4d"))
        .expect("admitted on its card");
    push_memory_with_total(&cpu_handle, CPU_RAM_MB / 2, 0, Some(CPU_RAM_MB), "ram");
    push_memory(&gpu_handle, 90_000, 0);

    let health = ledger.health();
    let device = |key: &str| {
        health
            .iter()
            .find(|gpu| gpu.gpu_uuid == key)
            .unwrap_or_else(|| panic!("{key} is on this host"))
    };
    assert_eq!(health.len(), 3, "two cards and the CPU device");
    assert_eq!(device("CPU").workers[0].inference_id, "g/cpu");
    assert_eq!(device("GPU-3c4d").workers[0].inference_id, "g/gpu");
    assert!(
        device("GPU-1a2b").workers.is_empty(),
        "the CPU replica is not charged to the GPU its pin named"
    );

    // Each device keeps its own regime.
    assert_eq!(device("CPU").total_mb, CPU_RAM_MB);
    assert_eq!(device("CPU").reserve_rule, RESERVE_RULE_RAM_FLOOR);
    assert_eq!(device("CPU").external_source.as_deref(), Some("ram"));
    assert!(
        device("CPU").limit_mb <= CPU_RAM_MB - cpu::ram_reserve_mb(CPU_RAM_MB)
            && device("CPU").limit_mb > 0,
        "limit {}",
        device("CPU").limit_mb
    );
    for card in ["GPU-1a2b", "GPU-3c4d"] {
        assert_ne!(device(card).reserve_rule, RESERVE_RULE_RAM_FLOOR, "{card}");
    }
    assert_eq!(device("GPU-3c4d").total_mb, 100_000);
    assert_eq!(device("GPU-3c4d").external_source.as_deref(), Some("nvml"));

    // Both are priced, and a measured window earns both a bigger budget.
    for (handle, admission) in [(&cpu_handle, &cpu_admission), (&gpu_handle, &gpu_admission)] {
        let first = measured_window(handle, admission, 4);
        let second = measured_window(handle, admission, 8);
        assert_eq!(first, 4, "the seed");
        assert!(second > first, "{first} -> {second}");
    }
    let health = ledger.health();
    for key in ["CPU", "GPU-3c4d"] {
        assert_eq!(
            device_of(&health, key).workers[0].knee_units,
            Some(16),
            "{key}"
        );
    }
}

/// One device's health row by key.
fn device_of<'a>(health: &'a [GpuBudgetHealth], key: &str) -> &'a GpuBudgetHealth {
    health
        .iter()
        .find(|gpu| gpu.gpu_uuid == key)
        .unwrap_or_else(|| panic!("{key} is on this host"))
}

/// Total adoption is an **MPS** mechanism, though a CPU device matches every
/// structural condition it has.
#[test]
fn a_cpu_devices_total_is_never_adopted_from_a_worker() {
    let ledger = cpu_ledger(no_margin());
    // Inside `(0, RAM]` and far outside the cross-check tolerance: the shape
    // that re-adopts on MPS.
    let handle = loaded_cpu(Some(CPU_RAM_MB / 2));
    assert!(
        ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .is_none(),
        "a report that disagrees with the GPU is refused, not adopted"
    );
    assert_eq!(
        ledger.health()[0].total_mb,
        CPU_RAM_MB,
        "the machine's RAM is not a number a worker gets to move"
    );
}

/// A replica that dies with a granted window in flight is a memory negative
/// on every **unified-memory** device, where an out-of-memory kill is an
/// uncatchable SIGKILL: it deflates the replica and halves the anchor, and
/// never reaches the fit. On private VRAM a death has too many other causes,
/// and an abort is not a death anywhere.
#[test]
fn a_death_mid_window_deflates_only_a_unified_device() {
    /// `(label, ledger, handle, gpu key, free sample, outcome, deflation, anchor)`.
    type DeathCase = (
        &'static str,
        Arc<VramLedger>,
        TelemetryHandle,
        &'static str,
        (u64, Option<u64>, &'static str),
        WindowOutcome,
        u32,
        u64,
    );
    let cases: Vec<DeathCase> = vec![
        (
            "a unified Apple GPU",
            mps_ledger(),
            loaded_mps(Some(MAC_RAM_MB / 4 * 3)),
            MPS_GPU,
            (60_000, None, "nvml"),
            WindowOutcome::WorkerDied,
            1,
            8,
        ),
        (
            "a unified ROCm GPU: an APU's memory is the machine's in exactly \
             the way that makes the Linux OOM killer the likely cause",
            apu_ledger(vec![apu_device(0)]),
            loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB)),
            AMD_A,
            (60_000, None, "nvml"),
            WindowOutcome::WorkerDied,
            1,
            8,
        ),
        (
            "a CPU-only host, where a death is the only memory signal there is",
            cpu_ledger(no_margin()),
            loaded_cpu(Some(CPU_RAM_MB)),
            "CPU",
            (40_000, Some(CPU_RAM_MB), "ram"),
            WindowOutcome::WorkerDied,
            1,
            8,
        ),
        (
            "a GPU with private VRAM: too many non-memory causes",
            ledger(100_000, no_margin()),
            loaded(Some(1000), Some(0)),
            GPU,
            (60_000, None, "nvml"),
            WindowOutcome::WorkerDied,
            0,
            16,
        ),
        (
            "an abort is not a death, even on a unified device",
            mps_ledger(),
            loaded_mps(Some(MAC_RAM_MB / 4 * 3)),
            MPS_GPU,
            (60_000, None, "nvml"),
            WindowOutcome::Aborted,
            0,
            16,
        ),
    ];
    for (label, ledger, handle, gpu, (free_mb, total_mb, source), outcome, deflation, anchor) in
        cases
    {
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory_with_total(&handle, free_mb, 0, total_mb, source);
        // A measured window moves the anchor to 16 units.
        measured_window(&handle, &admission, 16);
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            16,
            "{label}"
        );

        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(outcome);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.deflation, deflation, "{label}");
        assert_eq!(worker.max_units_measured, anchor, "{label}");
        assert_eq!(
            worker.death_cap_units.is_some(),
            deflation == 1,
            "{label}: capped exactly where the death is a negative"
        );
        assert_eq!(
            ledger
                .calibration_state("g/a", gpu)
                .map(|state| state.samples.len()),
            Some(1),
            "{label}: only the one real measurement reaches the fit"
        );
    }
}

/// The RAM basis on a machine whose size the fixture chooses.
pub(super) fn push_basis(
    handle: &TelemetryHandle,
    total_mb: u64,
    ram_total_mb: u64,
    ram_available_mb: u64,
    reserved_mb: u64,
    allocated_mb: u64,
) {
    let mut telemetry = handle.lock().unwrap();
    telemetry.memory = Some(Timestamped::now(MemorySample {
        free_mb: Some(ram_available_mb.min(total_mb)),
        total_mb: Some(total_mb),
        free_source: Some("mps".to_owned()),
        reserved_mb: Some(reserved_mb),
        allocated_mb: Some(allocated_mb),
        ram_total_mb: Some(ram_total_mb),
        ram_available_mb: Some(ram_available_mb),
    }));
}

/// A Mac of any size, with Metal's allocator.
fn mac_ledger(ram_mb: u64, recommended_max_mb: u64) -> Arc<VramLedger> {
    let ledger = VramLedger::for_test_gpus(
        &[(MPS_GPU, "Apple Silicon", recommended_max_mb, None)],
        // The shipped default: no user margin, so a capped 1 024 MiB reserve.
        VramBudget::default(),
        None,
    );
    {
        let mut state = ledger.lock();
        state.metal_allocator = true;
        state.gpus.get_mut(MPS_GPU).expect("the GPU").unified_ram_mb = Some(ram_mb);
    }
    ledger
}

/// `limit = min(recommended_max, memsize - external - reserve)`:
/// `recommended_max` already excludes the OS's share, so taking `external`
/// out of it too would carve the same pages out twice.
#[test]
fn a_36gb_mac_admits_the_ram_that_is_free_and_not_the_leftovers_of_a_ceiling() {
    const RAM: u64 = 36 * 1024;
    const RECOMMENDED_MAX: u64 = 27_648;
    const OS: u64 = 6 * 1024;
    const NEIGHBOUR: u64 = 22 * 1024;
    let ledger = mac_ledger(RAM, RECOMMENDED_MAX);
    let handle = loaded_mps(Some(RECOMMENDED_MAX));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    let available = RAM - OS - NEIGHBOUR;
    push_basis(&handle, RECOMMENDED_MAX, RAM, available, 0, 0);
    admission
        .request_grant(1, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    let gpu = &ledger.health()[0];
    assert_eq!(available, 8_192, "the machine can still give 8 GiB");
    // Our own resident's 1 000 MiB is a charge, not external usage.
    assert_eq!(
        gpu.external_mb,
        OS + NEIGHBOUR - 1_000,
        "priced out of hw.memsize and left there: 27 672, above the \
         device total, which the clip used to hide"
    );
    assert_eq!(gpu.reserve_mb, cpu::ram_reserve_mb(RAM), "the RAM floor");
    assert_eq!(
        gpu.limit_mb,
        available + 1_000 - gpu.reserve_mb,
        "the room the machine has, under a ceiling that is not binding"
    );
    let token = admission.request_grant(1, None, 1, 0).expect("granted");
    assert_eq!(
        token.grant().ram_reserve_mb,
        gpu.reserve_mb,
        "the worker's clamp keeps it too"
    );
    token.finish(WindowOutcome::Responded { oom: None });
}

/// The limit is the RAM domain's room under the allocator's ceiling, not
/// short of it by `hw.memsize - recommended_max_memory()`.
#[test]
fn the_limit_is_the_ram_domains_room_under_the_allocators_own_ceiling() {
    const RECOMMENDED_MAX: u64 = 122_880;
    const HOG: u64 = 99_968;
    let ledger = mac_ledger(MAC_RAM_MB, RECOMMENDED_MAX);
    let handle = loaded_mps(Some(RECOMMENDED_MAX));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    // External 101 453 with a 99 968 MiB hog: the rest is macOS's own pages.
    let ours = 1_000u64;
    let available = MAC_RAM_MB - 101_453 - ours;
    push_basis(&handle, RECOMMENDED_MAX, MAC_RAM_MB, available, 0, 0);
    admission
        .request_grant(1, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.external_mb, 101_453);
    assert_eq!(
        gpu.limit_mb, 16_512,
        "the RAM domain's room, not the 8 320 left short of the ceiling"
    );
    assert_eq!(
        MAC_RAM_MB - gpu.external_mb - gpu.reserve_mb - gpu.limit_mb,
        0,
        "nothing is lost to the gap between the two currencies"
    );
    assert!(HOG < gpu.external_mb);

    // Idle, under a ceiling raised past RAM less its reserve: the reserve
    // binds.
    push_basis(&handle, RECOMMENDED_MAX, MAC_RAM_MB, MAC_RAM_MB, 0, 0);
    admission
        .request_grant(1, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].limit_mb,
        MAC_RAM_MB - cpu::ram_reserve_mb(MAC_RAM_MB),
        "an idle Mac keeps its RAM reserve free"
    );

    // The limit reaches 0 when the RAM domain runs out.
    push_basis(&handle, RECOMMENDED_MAX, MAC_RAM_MB, 0, 0, 0);
    admission
        .request_grant(1, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    let gpu = &ledger.health()[0];
    assert_eq!(
        gpu.external_mb,
        MAC_RAM_MB - 1_000,
        "the whole machine is taken, ours apart"
    );
    assert_eq!(gpu.limit_mb, 0, "and the subtraction saturates there");
}

/// On a Mac the Metal and CPU devices are two views of one pool of RAM, so
/// each charges the other's residents: Σ limit must not exceed the machine,
/// and a CPU replica's growth must reach the Metal row, which refreshes from
/// MPS frames alone.
#[test]
fn the_unified_pair_charges_each_others_residents() {
    const RECMAX: u64 = MAC_RAM_MB / 4 * 3;
    /// The machine's own pages when the Metal frame was taken: few enough
    /// that Metal's ceiling, not the RAM, bounds the limit.
    const OTHERS: u64 = 18 * 1024;
    /// What the CPU replica grew to on top of its 1 000 MiB base.
    const CPU_GROWTH: u64 = 11_700;

    let ledger = VramLedger::new(
        &GpuInventory::known_mps(MAC_RAM_MB),
        no_margin().into(),
        None,
    );
    ledger.install_probe_stub(None);
    let row = |key: &str| {
        ledger
            .health()
            .into_iter()
            .find(|gpu| gpu.gpu_uuid == key)
            .unwrap_or_else(|| panic!("{key} is on this host"))
    };

    let mps_handle = loaded_mps(Some(RECMAX));
    let mps = ledger
        .register_worker("g/mps", item_cost(4), &mps_handle, Some(MPS_GPU))
        .expect("admitted on Metal");
    push_basis(
        &mps_handle,
        RECMAX,
        MAC_RAM_MB,
        MAC_RAM_MB - OTHERS - 1_000,
        0,
        0,
    );
    let metal_alone = row(MPS_GPU).headroom_mb;

    // A CPU replica on the same RAM, which never sends an MPS frame.
    let cpu_handle = loaded_on_cpu(Some(MAC_RAM_MB));
    let _cpu = ledger
        .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
        .expect("admitted on RAM");
    push_memory_with_total(
        &cpu_handle,
        MAC_RAM_MB - OTHERS - 1_000 - CPU_GROWTH,
        CPU_GROWTH,
        Some(MAC_RAM_MB),
        "ram",
    );

    let metal = row(MPS_GPU);
    let cpu = row(cpu::DEVICE_KEY);
    assert_eq!(cpu.charges_mb, 1_000 + CPU_GROWTH, "base plus growth");
    assert_eq!(
        metal_alone - metal.headroom_mb,
        cpu.charges_mb,
        "the Metal device lost exactly what the CPU replica holds"
    );
    // It is charged once: out of the Metal row's `external_mb`.
    assert_eq!(metal.external_mb, OTHERS - cpu.charges_mb);

    // On either device, what it still admits plus what the pair holds fits
    // in the RAM domain.
    let held = metal.charges_mb + cpu.charges_mb;
    for gpu in [&metal, &cpu] {
        assert!(
            gpu.headroom_mb + held <= MAC_RAM_MB - gpu.external_mb,
            "{}: {} + {held} > {}",
            gpu.gpu_uuid,
            gpu.headroom_mb,
            MAC_RAM_MB - gpu.external_mb
        );
    }

    // A grant on one is room the other no longer has.
    let before = row(cpu::DEVICE_KEY).headroom_mb;
    let grant = mps.request_grant(64, None, 1, 0).expect("granted");
    let metal = row(MPS_GPU);
    assert!(metal.grants_mb > 0, "the grant is outstanding");
    assert_eq!(
        before - row(cpu::DEVICE_KEY).headroom_mb,
        metal.grants_mb,
        "the CPU device lost the Metal grant"
    );
    grant.finish(WindowOutcome::Responded { oom: None });
}

/// The MPS and CPU devices of a Mac share its RAM, so a CPU replica counts
/// as a replica on the MPS device: a pre-fit MPS grant reserves half the
/// headroom and the CPU replica's window is still priced.
#[test]
fn a_pre_fit_mps_grant_leaves_ram_for_the_cpu_replica() {
    const RECMAX: u64 = MAC_RAM_MB / 4 * 3;
    let ledger = VramLedger::new(
        &GpuInventory::known_mps(MAC_RAM_MB),
        no_margin().into(),
        None,
    );
    ledger.install_probe_stub(None);
    let mps_handle = loaded_mps(Some(RECMAX));
    let on_mps = ledger
        .register_worker("g/mps", item_cost(4), &mps_handle, Some(MPS_GPU))
        .expect("admitted on Metal");
    let cpu_handle = loaded_on_cpu(Some(MAC_RAM_MB));
    let on_cpu = ledger
        .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
        .expect("admitted on RAM");
    // Nothing else holds RAM: both bases are ours.
    push_basis(&mps_handle, RECMAX, MAC_RAM_MB, MAC_RAM_MB - 2_000, 0, 0);
    ledger.ingest_all_for_test();
    let headroom = ledger.headroom_mb(MPS_GPU);

    let held = on_mps.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(held.grant().mb, headroom / 2);
    let other = on_cpu.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert!(other.grant().mb > 0);
    assert_eq!(other.grant().unit_budget, 4);
}

/// A load in flight on the CPU device of a Mac counts as a replica on the
/// MPS device too: the MPS replica's pre-fit grant is half of what the
/// load's reservation leaves.
#[tokio::test]
async fn a_load_on_the_cpu_device_of_a_mac_counts_on_the_mps_device() {
    const RECMAX: u64 = MAC_RAM_MB / 4 * 3;
    let ledger = VramLedger::new(
        &GpuInventory::known_mps(MAC_RAM_MB),
        no_margin().into(),
        None,
    );
    ledger.install_probe_stub(None);
    let mps_handle = loaded_mps(Some(RECMAX));
    let on_mps = ledger
        .register_worker("g/mps", item_cost(4), &mps_handle, Some(MPS_GPU))
        .expect("admitted on Metal");
    push_basis(&mps_handle, RECMAX, MAC_RAM_MB, MAC_RAM_MB - 1_000, 0, 0);
    ledger.ingest_all_for_test();
    ledger.record_free_for_test(cpu::DEVICE_KEY, MAC_RAM_MB - 1_000);
    let _loading = ledger
        .reserve_load_for_test("g/cpu", item_cost(4), cpu::DEVICE_KEY, None)
        .await
        .expect("the CPU device");
    let headroom = ledger.headroom_mb(MPS_GPU);
    assert_eq!(headroom, RECMAX - 1_000 - CONSERVATIVE_BASE_MB);

    let held = on_mps.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(held.grant().mb, headroom / 2);
}

/// With MPS sampled peaks, no batch reads warm off `peak_reserved`. Read off
/// the **post-batch** pool, the ring's warm batches are told from the ones
/// that grew it: wd-vit's rate is no better at 128 units than at 64, nor at
/// 256, and within 5 % of its best down to 4 units, where the job ends.
#[test]
fn a_long_job_of_sampled_mps_windows_ends_at_the_smallest_size_near_its_best_rate() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let mut budgets = Vec::new();
    for _ in 0..1_200 {
        budgets.push(mps_sampled_window(&handle, &admission, &WDVIT_M3_MAX));
    }
    let worker = &ledger.health()[0].workers[0];
    assert!(
        worker.throughput_samples > 0,
        "the sampler's peak does not disqualify every batch"
    );
    assert_eq!(worker.knee_units, Some(4));
    assert_eq!(
        (budgets[0], budgets[2], budgets.iter().copied().max()),
        (64, 128, Some(256)),
        "one size above was tried, one doubling past it, and no more: {:?}",
        &budgets[..12]
    );
    assert_eq!(*budgets.last().expect("windows"), 4);
}

/// Three warm windows ahead of the same job change nothing.
#[test]
fn three_warm_windows_do_not_decide_the_budget_for_the_whole_job() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    for _ in 0..3 {
        ramp_window(&handle, &admission, &WDVIT_M3_MAX);
    }
    let mut budgets = Vec::new();
    for _ in 0..1_200 {
        budgets.push(mps_sampled_window(&handle, &admission, &WDVIT_M3_MAX));
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.knee_units, Some(4));
    assert_eq!(*budgets.last().expect("windows"), 4);
    assert!(budgets.iter().all(|units| *units <= 256));
}
/// The **ceiling** half of `limit = min(recommended_max, memsize - external -
/// reserve)`, swept: wherever more RAM is free than Metal will hand out, the
/// published limit and the grant are Metal's figure.
#[test]
fn the_allocators_ceiling_binds_wherever_free_ram_is_the_looser_term() {
    const RECOMMENDED_MAX: u64 = 98_304;
    let ledger = mac_ledger(MAC_RAM_MB, RECOMMENDED_MAX);
    let handle = loaded_mps(Some(RECOMMENDED_MAX));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    let (mut ceiling_bound, mut room_bound) = (0u32, 0u32);
    for available in (16_384..=126_976).step_by(4_096) {
        push_basis(&handle, RECOMMENDED_MAX, MAC_RAM_MB, available, 0, 0);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let mb = token.grant().mb;
        token.finish(WindowOutcome::Responded { oom: None });
        let gpu = &ledger.health()[0];
        // `ours` is the 1 000 MiB base with no pool on top of it.
        assert_eq!(gpu.external_mb, MAC_RAM_MB - available - 1_000);
        let room = MAC_RAM_MB - gpu.external_mb - gpu.reserve_mb;
        assert_eq!(
            gpu.limit_mb,
            room.min(RECOMMENDED_MAX),
            "available {available}, room {room}, reserve {}",
            gpu.reserve_mb
        );
        assert!(
            mb <= RECOMMENDED_MAX,
            "a grant of {mb} MiB past the allocator's own ceiling"
        );
        if room > RECOMMENDED_MAX {
            ceiling_bound += 1;
            assert_eq!(gpu.limit_mb, RECOMMENDED_MAX, "available {available}");
        } else {
            room_bound += 1;
            assert_eq!(gpu.limit_mb, room, "available {available}");
        }
    }
    assert!(
        ceiling_bound >= 5 && room_bound >= 5,
        "the sweep must cross the point where the terms swap: \
         {ceiling_bound} ceiling-bound, {room_bound} room-bound"
    );
}
/// The term netted out of `external` is the **driver pool** figure —
/// `base + (reserved_now - reserved_at_load)` — with no margin in it, and
/// it does not move when live tensors are freed into the pool.
#[test]
fn the_netted_term_is_the_pool_and_carries_no_margin() {
    const RECMAX: u64 = 122_880;
    const TAKEN: u64 = 112_937;
    let available = MAC_RAM_MB - TAKEN;
    // A user margin of 4.0 must not move `external`.
    for budget in [no_margin(), user_margin(4.0)] {
        let ledger =
            VramLedger::for_test_gpus(&[(MPS_GPU, "Apple Silicon", RECMAX, None)], budget, None);
        {
            let mut state = ledger.lock();
            state.metal_allocator = true;
            state.gpus.get_mut(MPS_GPU).expect("the GPU").unified_ram_mb = Some(MAC_RAM_MB);
        }
        let handle = loaded_mps(Some(RECMAX));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        // Pool 2 000, live 40: the pool is the subtrahend, whatever is live.
        for (pool, live) in [(2_000u64, 40u64), (2_000, 1_800), (2_000, 0)] {
            push_basis(&handle, RECMAX, MAC_RAM_MB, available, pool, live);
            admission
                .request_grant(1, None, 1, 0)
                .expect("granted")
                .finish(WindowOutcome::Responded { oom: None });
            let gpu = &ledger.health()[0];
            assert_eq!(
                gpu.external_mb,
                TAKEN - (1_000 + pool),
                "external nets base+pool only: pool {pool}, live {live}"
            );
        }
    }
}

/// `external` nets our pool, and `charges_locked` puts the same pool back,
/// so the growth admitted is `available - reserve`, never
/// `available + pool - reserve`.
#[test]
fn the_pool_is_in_the_room_and_in_the_charge_so_only_free_ram_is_admitted() {
    const RECMAX: u64 = 122_880;
    const HOG: u64 = 98_688;
    // External 112 937 against a footprint of 2 244.
    const OURS: u64 = 2_244;
    const EXTERNAL: u64 = 112_937;
    let available = MAC_RAM_MB - EXTERNAL - OURS;
    assert_eq!(available, 15_891, "the RAM the machine actually has free");
    assert!(
        EXTERNAL > std::hint::black_box(HOG),
        "macOS's own pages are real external usage on top of the hog"
    );
    let ledger = mac_ledger(MAC_RAM_MB, RECMAX);
    let handle = loaded_mps(Some(RECMAX));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    // base 1 000 at load, so 1 244 of pool growth makes the 2 244.
    push_basis(&handle, RECMAX, MAC_RAM_MB, available, 1_244, 1_000);
    admission
        .request_grant(1, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.external_mb, EXTERNAL);
    assert_eq!(gpu.reserve_mb, cpu::ram_reserve_mb(MAC_RAM_MB), "the RAM floor");
    assert_eq!(
        gpu.limit_mb,
        available + OURS - gpu.reserve_mb,
        "the room credits the pool once"
    );
    assert_eq!(
        gpu.headroom_mb,
        available - gpu.reserve_mb,
        "and the charge takes it back: new growth is bounded by free RAM"
    );
    // The published grant never asks past it either.
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert!(
        token.grant().mb <= available - gpu.reserve_mb + 1_244,
        "a grant may spend our own free pool, never other people's RAM: {}",
        token.grant().mb
    );
    token.finish(WindowOutcome::Responded { oom: None });
}

/// A Mac replica ramped 4 → 64 on an idle machine, its next budget 128.
fn ramped_mac_replica() -> (Arc<VramLedger>, TelemetryHandle, Admission) {
    let ledger = mps_ledger();
    let handle = loaded_mps(Some(MAC_TOTAL_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_ram(&handle, MAC_TOTAL_MB, 90_000, 0, 0);
    let ramped: Vec<u64> = (0..6)
        .map(|_| ramp_window(&handle, &admission, &MINILM_M3_MAX))
        .collect();
    assert_eq!(ramped, [4, 4, 8, 16, 32, 64]);
    (ledger, handle, admission)
}

/// `recommended_max_memory()` of the Mac in [`ramped_mac_replica`].
const MAC_TOTAL_MB: u64 = 110_100;

/// `(working size, deflation, throughput_samples, unit_budget)` of the replica.
fn ramp_figures(ledger: &Arc<VramLedger>) -> (Option<u64>, u32, usize, u64) {
    let health = ledger.health();
    let worker = &health[0].workers[0];
    (
        worker.knee_units,
        worker.deflation,
        worker.throughput_samples,
        worker.unit_budget,
    )
}

/// What paging left of the replica's batch size, if anything.
fn pressure_cap(ledger: &Arc<VramLedger>) -> Option<PressureCap> {
    ledger.lock().calibration[&("g/a".to_owned(), MPS_GPU.to_owned())].pressure_cap
}

/// `windows` windows while macOS pages: the worker reads nothing available
/// above the RAM reserve and holds 180 MiB of pool, which is 8 units.
fn paging_windows(
    ledger: &Arc<VramLedger>,
    handle: &TelemetryHandle,
    admission: &Admission,
    windows: usize,
) {
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Paging);
    push_ram(handle, MAC_TOTAL_MB, cpu::ram_reserve_mb(MAC_RAM_MB), 180, 0);
    for window in 0..windows {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let grant = *token.grant();
        assert_eq!((grant.unit_budget, grant.mb), (8, 180));
        assert_eq!(grant.squeezed, window == 0, "cut once, then held there");
        // A warm batch, then collapses the pool growth would corroborate on
        // an idle machine.
        let mut batches = vec![measurement(8, 0, 180), warm_batch(8, 100.0)];
        batches.extend((2..WINDOW_DEPTH_MULTIPLIER).map(|_| spilled_past_free(8, 100.0, 0)));
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
    }
}

/// A window that runs out of memory while macOS pages was cut by the
/// machine's pressure, not by the price: the pool margin stays. The same
/// window at warning with nothing paged out, or without pressure, raises
/// it: nothing else would stop it failing again.
#[test]
fn an_out_of_memory_window_while_the_mac_pages_leaves_the_pool_margin() {
    let out_of_memory = WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Marker),
    };
    for (pressure, raised) in [
        (mps::MemoryPressure::Critical, 0),
        (mps::MemoryPressure::Paging, 0),
        (mps::MemoryPressure::Warning, 1),
        (mps::MemoryPressure::Normal, 1),
    ] {
        let (ledger, handle, admission) = ramped_mac_replica();
        ledger.set_memory_pressure_for_test(pressure);
        push_ram(&handle, MAC_TOTAL_MB, cpu::ram_reserve_mb(MAC_RAM_MB), 180, 0);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let grant = *token.grant();
        assert_eq!(
            (grant.unit_budget, grant.mb, grant.squeezed),
            (8, 180, true)
        );
        token.finish(out_of_memory);
        assert_eq!(
            margin_steps(&ledger, "g/a", MPS_GPU),
            raised,
            "{pressure:?}"
        );
    }
}

/// `windows` full windows on an idle-looking machine, and their unit budgets.
fn ramp_windows(handle: &TelemetryHandle, admission: &Admission, windows: usize) -> Vec<u64> {
    push_ram(handle, MAC_TOTAL_MB, 90_000, 180, 0);
    (0..windows)
        .map(|_| ramp_window(handle, admission, &MINILM_M3_MAX))
        .collect()
}

/// While the Mac pages the reading leaves nothing available, so a grant is
/// cut to the pool the replica holds. Those windows earn no ramp step, feed
/// no knee and deflate nothing. At normal the batch grows back from the size
/// it ran at by doubling, not in one jump.
#[test]
fn while_the_mac_pages_a_grant_fits_the_pool_held_and_grows_back_by_doubling() {
    let (ledger, handle, admission) = ramped_mac_replica();
    let (_, _, samples, budget) = ramp_figures(&ledger);
    assert_eq!(budget, 128, "the trial's next size");
    paging_windows(&ledger, &handle, &admission, 3);
    let (size_during, deflation, samples_during, _) = ramp_figures(&ledger);
    assert_eq!(
        size_during,
        Some(64),
        "the trial is put off at the size it had measured; no paging window earned one"
    );
    assert_eq!(deflation, 0, "no collapse was counted");
    assert!(
        samples_during <= samples,
        "no rate reached the throughput ring"
    );

    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Normal);
    push_ram(&handle, MAC_TOTAL_MB, 90_000, 180, 0);
    // A window the queue sized did not fill the size, so it earns no doubling.
    queued_window_at_the_rate(&handle, &admission, 3, |_| 100.0);
    assert_eq!(ramp_figures(&ledger).3, 8);
    assert_eq!(ramp_windows(&handle, &admission, 4), [8, 16, 32, 64]);
    assert_eq!(pressure_cap(&ledger), None, "back at the working size");
    assert_eq!(
        ramp_windows(&handle, &admission, 2),
        [64, 64],
        "the trial that was put off waits its twelve windows"
    );
}

/// At warning, once the paging has stopped, the batch grows back by doubling
/// to half the size the episode began at and no further; a second episode
/// halves that bound again. The full size returns only at normal.
#[test]
fn at_warning_after_paging_the_batch_regrows_to_half_the_size_paging_began_at() {
    use mps::MemoryPressure::{Normal, Warning};
    let (ledger, handle, admission) = ramped_mac_replica();
    paging_windows(&ledger, &handle, &admission, 2);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(
        ramp_windows(&handle, &admission, 5),
        [8, 16, 32, 64, 64],
        "half of the 128 in force when the paging began"
    );
    paging_windows(&ledger, &handle, &admission, 2);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(ramp_windows(&handle, &admission, 4), [8, 16, 32, 32]);
    assert_eq!(
        ramp_figures(&ledger).0,
        Some(64),
        "no pressure window earned a size"
    );

    ledger.set_memory_pressure_for_test(Normal);
    assert_eq!(ramp_windows(&handle, &admission, 4), [32, 64, 64, 64]);
    assert_eq!(pressure_cap(&ledger), None);
}

/// The bound lasts until the batch is back at what the ramp admits. A
/// warning that returns before then grows back to the same bound, and holds
/// there; an episode that begins before then halves the bound again.
#[test]
fn a_warning_or_an_episode_before_the_batch_is_back_keeps_the_bound() {
    use mps::MemoryPressure::{Normal, Warning};
    let (ledger, handle, admission) = ramped_mac_replica();
    paging_windows(&ledger, &handle, &admission, 1);
    ledger.set_memory_pressure_for_test(Normal);
    assert_eq!(ramp_windows(&handle, &admission, 2), [8, 16]);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(
        ramp_windows(&handle, &admission, 3),
        [32, 64, 64],
        "half of the 128 in force when the paging began"
    );

    ledger.set_memory_pressure_for_test(Normal);
    assert_eq!(ramp_windows(&handle, &admission, 1), [64]);
    assert_eq!(pressure_cap(&ledger), None, "back at what the ramp admits");
    paging_windows(&ledger, &handle, &admission, 1);
    ledger.set_memory_pressure_for_test(Normal);
    assert_eq!(ramp_windows(&handle, &admission, 1), [8]);
    paging_windows(&ledger, &handle, &admission, 1);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(
        ramp_windows(&handle, &admission, 4),
        [8, 16, 16, 16],
        "half of the 64 in force when this paging began, halved again"
    );
}

/// The bound is at least one unit, or a batch already at one unit would be
/// capped at none and never grow back.
#[test]
fn the_bound_of_a_one_unit_batch_is_one_unit() {
    let ledger = mps_ledger();
    let handle = loaded_mps(Some(MAC_TOTAL_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(1), &handle, None)
        .expect("registers");
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Paging);
    push_ram(&handle, MAC_TOTAL_MB, 0, 0, 0);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().unit_budget, 1);
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(1, 0, 10)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        pressure_cap(&ledger),
        Some(PressureCap {
            units: 1,
            regrow_to: 1,
            paging: true,
        })
    );
}

/// The size paging left belongs to the model on the device, so a replica
/// loaded afterwards runs what the one that lived through it runs.
#[test]
fn a_reloaded_replica_inherits_the_size_paging_left() {
    let (ledger, handle, admission) = ramped_mac_replica();
    paging_windows(&ledger, &handle, &admission, 1);
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Warning);
    let reloaded = loaded_mps(Some(MAC_TOTAL_MB));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &reloaded, None)
        .expect("registers");
    let budgets: Vec<u64> = ledger.health()[0]
        .workers
        .iter()
        .map(|worker| worker.unit_budget)
        .collect();
    assert_eq!(budgets, [8, 8]);
    assert_eq!(
        ledger.window_target_units(admission.worker_id()),
        8 * WINDOW_DEPTH_MULTIPLIER,
        "the dispatcher fills windows for the size kept"
    );
}

/// Only a paging window that memory or the ramp sized sets the size kept: not
/// one the queue sized, unless memory cut that too. The size kept is a
/// ceiling: a knee below it still decides.
#[test]
fn a_paging_window_the_queue_sized_does_not_set_the_size_kept() {
    let (ledger, handle, admission) = ramped_mac_replica();
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Paging);
    queued_window_at_the_rate(&handle, &admission, 5, |_| 100.0);
    assert_eq!(
        pressure_cap(&ledger),
        None,
        "5 units of work, room for more"
    );

    push_ram(&handle, MAC_TOTAL_MB, cpu::ram_reserve_mb(MAC_RAM_MB), 180, 0);
    let granted = queued_window_at_the_rate(&handle, &admission, 20, |_| 100.0);
    assert_eq!(granted, 8, "20 units of work, memory for 8");
    assert_eq!(pressure_cap(&ledger).map(|cap| cap.units), Some(8));

    ledger.set_knee_for_test("g/a", MPS_GPU, 3);
    assert_eq!(ramp_figures(&ledger).3, 3);
}

/// At warning with nothing being paged out the replica keeps its working
/// size: the trial of the next one is put off, and there is no growth and
/// no throughput sample. A squeeze there is not kept once its cause is
/// gone. The trial is taken up again after the pressure ends, within two
/// doublings of the working size.
#[test]
fn at_warning_without_paging_the_batch_size_is_held() {
    let (ledger, handle, admission) = ramped_mac_replica();
    let samples = ramp_figures(&ledger).2;
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Warning);
    let held: Vec<u64> = (0..3)
        .map(|_| ramp_window(&handle, &admission, &MINILM_M3_MAX))
        .collect();
    assert_eq!(held, [128, 64, 64], "the trial under way is put off");
    let (size_during, _, samples_during, _) = ramp_figures(&ledger);
    assert_eq!(size_during, Some(64), "what the trial had measured");
    assert!(samples_during <= samples);
    assert_eq!(
        ledger.trial_for_test("g/a", MPS_GPU),
        (None, RETEST_WINDOWS, 0),
        "put off, not counted as a trial that left the size in place"
    );
    let reached =
        ledger.lock().calibration[&("g/a".to_owned(), MPS_GPU.to_owned())].max_units_measured_here;
    assert_eq!(
        reached, 64,
        "a size run under pressure is not one the ramp reached"
    );

    // Memory for 8 units for one window, then room again.
    push_ram(&handle, MAC_TOTAL_MB, cpu::ram_reserve_mb(MAC_RAM_MB), 180, 0);
    assert_eq!(ramp_window(&handle, &admission, &MINILM_M3_MAX), 8);
    assert_eq!(
        pressure_cap(&ledger),
        None,
        "a squeeze, not a paging episode"
    );
    assert_eq!(ramp_windows(&handle, &admission, 1), [64]);

    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Normal);
    let after = ramp_windows(&handle, &admission, RETEST_WINDOWS as usize + 3);
    assert_eq!(
        after[..RETEST_WINDOWS as usize],
        [64; RETEST_WINDOWS as usize]
    );
    assert_eq!(after[RETEST_WINDOWS as usize..], [128, 256, 256]);
}

/// Pressure at either end of a window marks it: at the grant only, or at the
/// settle only.
#[test]
fn a_window_under_pressure_at_either_end_earns_no_step() {
    use mps::MemoryPressure::{Critical, Normal, Warning};
    for (at_grant, at_settle) in [(Warning, Normal), (Normal, Critical)] {
        let (ledger, handle, admission) = ramped_mac_replica();
        ledger.set_memory_pressure_for_test(at_grant);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        ledger.set_memory_pressure_for_test(at_settle);
        let rate = ladder_rate(&MINILM_M3_MAX, 128);
        let mut batches = vec![BatchMeasurement {
            duration_ms: Some(128.0 * 1000.0 / rate),
            ..measurement(128, 0, 10 * 128 + 100)
        }];
        batches.extend((1..WINDOW_DEPTH_MULTIPLIER).map(|_| warm_batch(128, rate)));
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ramp_figures(&ledger).0,
            Some(64),
            "128 units earned nothing: {at_grant:?} at the grant, {at_settle:?} at the settle"
        );
    }
}

/// The cap a death left and the cap paging left bound the batch together,
/// the smaller ruling, and each ends on its own terms. A worker that dies
/// while the Mac pages sets the death cap like any other death; the paging
/// cap lifts once the batch has grown back, the death cap stays.
#[test]
fn a_death_cap_and_a_paging_cap_hold_the_smaller_batch() {
    let (ledger, handle, admission) = ramped_mac_replica();
    paging_windows(&ledger, &handle, &admission, 2);
    let paged = pressure_cap(&ledger).expect("capped by the paging windows");
    assert_eq!(paged.units, 8);

    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().unit_budget, 8);
    token.finish(WindowOutcome::WorkerDied);
    assert_eq!(
        pressure_cap(&ledger),
        Some(paged),
        "a death leaves it alone"
    );
    drop(admission);

    // The model is reloaded while the Mac still pages.
    let handle = loaded_mps(Some(MAC_TOTAL_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    let worker = |ledger: &Arc<VramLedger>| ledger.health().swap_remove(0).workers.swap_remove(0);
    assert_eq!(worker(&ledger).death_cap_units, Some(4));
    assert_eq!(worker(&ledger).unit_budget, 4, "the smaller of 4 and 8");

    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Normal);
    assert_eq!(ramp_windows(&handle, &admission, 4), [4, 4, 4, 4]);
    assert_eq!(pressure_cap(&ledger), None, "back at what is admitted");
    assert_eq!(worker(&ledger).death_cap_units, Some(4), "until restart");
}
