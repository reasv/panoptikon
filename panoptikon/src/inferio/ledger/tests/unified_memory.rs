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
        gtt: None,
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
        gtt: None,
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
pub(super) const APU_TOTAL_MB: u64 = APU_CARVEOUT_MB + 64 * 1024;

/// An APU row as `rocm.rs` builds one, at `0000:03:00.0`.
pub(super) fn apu_device(index: u32) -> crate::inferio::gpu::GpuInfo {
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

/// An APU's memory sample: free VRAM plus the smaller of unclaimed GTT and
/// deliverable RAM, with both terms beside it.
fn apu_sample(
    vram_free_mb: u64,
    gtt_free_mb: u64,
    ram_available_mb: u64,
    reserved_mb: u64,
) -> MemorySample {
    MemorySample {
        free_mb: Some(vram_free_mb + gtt_free_mb.min(ram_available_mb)),
        free_source: Some("amdgpu-sysfs".to_owned()),
        reserved_mb: Some(reserved_mb),
        allocated_mb: Some(reserved_mb),
        ram_available_mb: Some(ram_available_mb),
        gtt_free_mb: Some(gtt_free_mb),
        ..MemorySample::default()
    }
}

/// [`apu_sample`] as the replica's memory frame.
pub(super) fn push_apu(
    handle: &TelemetryHandle,
    vram_free_mb: u64,
    gtt_free_mb: u64,
    ram_available_mb: u64,
    reserved_mb: u64,
) {
    let sample = apu_sample(vram_free_mb, gtt_free_mb, ram_available_mb, reserved_mb);
    handle.lock().unwrap().memory = Some(Timestamped::now(sample));
}

pub(super) fn apu_ledger(gpus: Vec<crate::inferio::gpu::GpuInfo>) -> Arc<VramLedger> {
    VramLedger::new(
        &GpuInventory::known_rocm(gpus),
        VramBudget::default().into(),
        None,
    )
}

/// `gpus` beside a CPU device of `cpu_total_mb`, with the probe stubbed out.
fn apu_host(
    gpus: Vec<crate::inferio::gpu::GpuInfo>,
    cpu_total_mb: u64,
    budgets: impl Into<VramBudgets>,
) -> Arc<VramLedger> {
    let inventory = GpuInventory::known_rocm(gpus)
        .with_cpu(cpu_total_mb, crate::inferio::cpu::MemRoots::default());
    let ledger = VramLedger::new(&inventory, budgets.into(), None);
    ledger.install_probe_stub(None);
    ledger
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
        // With no reading, the RAM floor of the RAM the OS manages.
        assert_eq!(
            gpu.limit_mb,
            APU_TOTAL_MB - cpu::ram_reserve_mb(128 * 1024 - APU_CARVEOUT_MB)
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
fn a_configured_ceiling_caps_the_cpu_device() {
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

/// On a CPU host a worker reaches the CPU device by reporting
/// `device_kind = cpu`. A GPU worker without a UUID is never priced against
/// RAM, even when its total matches RAM (an APU, or a 64 GB card in a 64 GB
/// host).
#[test]
fn only_a_cpu_report_registers_against_the_ram_device() {
    let ledger = cpu_ledger(no_margin());
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &loaded_cpu(Some(CPU_RAM_MB)), None)
        .expect("a CPU worker");
    assert_eq!(
        admitted_gpu(&ledger, 0),
        ("CPU".to_owned(), "g/a".to_owned())
    );

    for device_kind in [Some("rocm"), None] {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            gpu_bdf: Some("0000:03:00.0".to_owned()),
            gpu_total_mb: Some(CPU_RAM_MB),
            device_kind: device_kind.map(str::to_owned),
            ..LoadReport::default()
        }));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        assert!(
            cpu_ledger(no_margin())
                .register_worker("g/a", item_cost(4), &handle, None)
                .is_none(),
            "{device_kind:?}"
        );
    }
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
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("a CPU worker is placed by its device kind");
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
        gtt_free_mb: None,
    }));
}

/// A Mac of any size, with Metal's allocator.
fn mac_ledger(ram_mb: u64, recommended_max_mb: u64) -> Arc<VramLedger> {
    let ledger = VramLedger::for_test_gpus(
        &[(MPS_GPU, "Apple Silicon", recommended_max_mb, None)],
        // The shipped default: no user margin, so the RAM floor as reserve.
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
    assert_eq!(
        ledger.refusal_room_locked(&ledger.lock(), MPS_GPU),
        MAC_RAM_MB - cpu::ram_reserve_mb(MAC_RAM_MB)
    );
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

/// Two APUs and the CPU device draw on the same RAM, each APU beyond its
/// carve-out. While RAM, not the GTT side, binds the APUs, every device
/// reads the same external usage, an APU's limit stays within its total, and
/// what a grant adds to its device's charges is room the others no longer
/// have, before any new reading. Each grant carries its own device's
/// reserve, which on an APU is the RAM floor, to the worker.
#[test]
fn apus_and_the_cpu_device_charge_each_others_grants() {
    const RAM: u64 = 128 * 1024 - APU_CARVEOUT_MB;
    const OTHERS: u64 = 40 * 1024;
    const POOL: u64 = 2048;
    const CPU_POOL: u64 = 60 * 1024;
    let apu_b = crate::inferio::gpu::GpuInfo {
        index: 1,
        uuid: AMD_B.to_owned(),
        bdf: Some("0000:0c:00.0".to_owned()),
        ..apu_device(1)
    };
    let ledger = apu_host(vec![apu_device(0), apu_b], RAM, VramBudget::default());
    let mut replicas = Vec::new();
    for (model, handle, device) in [
        (
            "g/a",
            loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB)),
            AMD_A,
        ),
        (
            "g/b",
            loaded_rocm(Some("0000:0c:00.0"), Some(APU_TOTAL_MB)),
            AMD_B,
        ),
        ("g/cpu", loaded_on_cpu(Some(RAM)), cpu::DEVICE_KEY),
    ] {
        let admission = ledger
            .register_worker(model, item_cost(4), &handle, Some(device))
            .expect("admitted");
        replicas.push((admission, handle, device));
    }
    // Each APU's carve-out is full; the rest of its footprint is in RAM.
    let footprint = 1_000 + POOL;
    let available = RAM - OTHERS - (1_000 + CPU_POOL) - 2 * (footprint - APU_CARVEOUT_MB);
    for (_, handle, device) in &replicas {
        if *device == cpu::DEVICE_KEY {
            push_memory_with_total(handle, available, CPU_POOL, Some(RAM), "ram");
        } else {
            push_apu(handle, 0, 60 * 1024, available, POOL);
        }
    }
    ledger.ingest_all_for_test();

    let health = ledger.health();
    for (_, _, device) in &replicas {
        let row = device_of(&health, device);
        assert_eq!(row.external_mb, OTHERS, "{device}");
        assert!(row.limit_mb <= row.total_mb, "{device}");
    }
    let reserve = |key: &str| device_of(&ledger.health(), key).reserve_mb;
    let charges = |key: &str| device_of(&ledger.health(), key).charges_mb;
    assert_eq!(reserve(AMD_A), cpu::ram_reserve_mb(RAM));
    for (admission, _, own) in &replicas {
        let others: Vec<(&str, u64)> = replicas
            .iter()
            .filter(|(_, _, device)| device != own)
            .map(|(_, _, device)| (*device, ledger.headroom_mb(device)))
            .collect();
        let charged = charges(own);
        let grant = admission.request_grant(64, None, 1, 0).expect("granted");
        let charged = charges(own) - charged;
        assert!(charged > 0, "{own}");
        for (other, before) in others {
            assert_eq!(
                before - ledger.headroom_mb(other),
                charged,
                "{other} lost the grant on {own}"
            );
        }
        assert_eq!(grant.grant().ram_reserve_mb, reserve(own));
        grant.finish(WindowOutcome::Responded { oom: None });
    }
}

/// An APU whose GTT side binds holds only its own memory there: a CPU
/// replica that fills RAM the GTT side does not need leaves its headroom as
/// it was, by whichever route the APU's next reading arrives. Its grant still
/// carries the RAM floor to the worker.
#[test]
fn a_cpu_replica_leaves_an_apus_gtt_side_alone() {
    const RAM: u64 = 128 * 1024 - APU_CARVEOUT_MB;
    const CPU_POOL: u64 = 30 * 1024;
    // 1 GiB of other usage; the APU's 1 000 MiB base fills its carve-out.
    let gtt_free = 64 * 1024 - (1_000 - APU_CARVEOUT_MB);
    for route in ["frame", "pool refresh", "batch", "probe", "load report"] {
        let ledger = apu_host(vec![apu_device(0)], RAM, VramBudget::default());
        let apu_handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
        let on_apu = ledger
            .register_worker("g/apu", item_cost(4), &apu_handle, None)
            .expect("admitted on the APU");
        let available = RAM - 1024 - (1_000 - APU_CARVEOUT_MB);
        push_apu(&apu_handle, 0, gtt_free, available, 0);
        ledger.ingest_all_for_test();
        let alone = ledger.headroom_mb(AMD_A);

        let cpu_handle = loaded_on_cpu(Some(RAM));
        let _on_cpu = ledger
            .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
            .expect("admitted on RAM");
        let available = available - 1_000 - CPU_POOL;
        assert!(available > gtt_free, "RAM has more room than the GTT side");
        push_memory_with_total(&cpu_handle, available, CPU_POOL, Some(RAM), "ram");
        ledger.ingest_all_for_test();
        let charges = || device_of(&ledger.health(), AMD_A).charges_mb;
        let before = charges();
        let mut held = None;
        match route {
            "frame" => {
                push_apu(&apu_handle, 0, gtt_free, available, 0);
                ledger.ingest_all_for_test();
            }
            // Read by `health`, which refreshes pools first.
            "pool refresh" => push_apu(&apu_handle, 0, gtt_free, available, 0),
            "batch" => {
                let token = on_apu.request_grant(1, None, 1, 0).expect("granted");
                apu_handle
                    .lock()
                    .unwrap()
                    .record_measurements(vec![BatchMeasurement {
                        free_mb: Some(gtt_free),
                        free_source: Some("amdgpu-sysfs".to_owned()),
                        gtt_free_mb: Some(gtt_free),
                        ram_available_mb: Some(available),
                        ..measurement(1, 0, 0)
                    }]);
                token.finish(WindowOutcome::Responded { oom: None });
                ledger.ingest_all_for_test();
            }
            "probe" => {
                ledger.install_probe_stub(Some(vec![GpuMemory {
                    uuid: AMD_A.to_owned(),
                    total_mb: APU_TOTAL_MB,
                    free_mb: gtt_free,
                    gtt: Some(crate::inferio::gpu::GttBasis {
                        gtt_free_mb: gtt_free,
                        ram_available_mb: available,
                    }),
                }]));
                held = on_apu.request_grant(64, None, 1, 0);
                assert_eq!(ledger.probe_calls(), 1);
            }
            // A second replica's 1 000 MiB base, in GTT.
            _ => {
                let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
                let sample = apu_sample(0, gtt_free - 1_000, available - 1_000, 0);
                handle.lock().unwrap().load.as_mut().unwrap().value.memory = Some(sample);
                ledger
                    .register_worker("g/apu2", item_cost(4), &handle, None)
                    .expect("admitted on the APU");
            }
        }
        let health = ledger.health();
        let apu = device_of(&health, AMD_A);
        assert_eq!(
            apu.headroom_mb + (apu.charges_mb - before),
            alone,
            "{route}"
        );
        // The GTT side binds under the GPU's reserve; the worker still keeps
        // the RAM floor out of the RAM term of its reading.
        assert_eq!(apu.reserve_rule, RESERVE_RULE_GPU_FLOOR, "{route}");
        let grant = held.unwrap_or_else(|| on_apu.request_grant(64, None, 1, 0).expect("granted"));
        assert_eq!(
            grant.grant().ram_reserve_mb,
            cpu::ram_reserve_mb(RAM),
            "{route}"
        );
    }
}

/// A device's `cap_fraction` bounds its own memory: an APU's 32 GiB pool in
/// GTT, with RAM to spare, leaves the CPU device's headroom under a cap of a
/// quarter of RAM as it was.
#[test]
fn an_apu_pool_leaves_the_cpu_devices_cap_alone() {
    const RAM: u64 = 128 * 1024 - APU_CARVEOUT_MB;
    const POOL: u64 = 32 * 1024;
    const OTHERS: u64 = 10 * 1024;
    let budgets = VramBudgets::uniform(VramBudget::default()).with_gpu(
        cpu::DEVICE_KEY,
        VramBudget {
            cap_fraction: Some(0.25),
            ..VramBudget::default()
        },
    );
    let ledger = apu_host(vec![apu_device(0)], RAM, budgets);
    let cpu_handle = loaded_on_cpu(Some(RAM));
    let _on_cpu = ledger
        .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
        .expect("admitted on RAM");
    push_memory_with_total(&cpu_handle, RAM - OTHERS - 1_000, 0, Some(RAM), "ram");
    ledger.ingest_all_for_test();
    let alone = ledger.headroom_mb(cpu::DEVICE_KEY);

    let apu_handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
    let _on_apu = ledger
        .register_worker("g/apu", item_cost(4), &apu_handle, None)
        .expect("admitted on the APU");
    let in_ram = 1_000 + POOL - APU_CARVEOUT_MB;
    let available = RAM - OTHERS - 1_000 - in_ram;
    push_memory_with_total(&cpu_handle, available, 0, Some(RAM), "ram");
    push_apu(&apu_handle, 0, 64 * 1024 - in_ram, available, POOL);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(cpu::DEVICE_KEY), alone);
}

/// In a container limited to 16 GiB on a 128 GB APU host, the APU's RAM
/// side is the carve-out plus the container's RAM, and its floor is the
/// container's, as the CPU device's is, whatever the margin. The floor comes
/// off the RAM term only: with less RAM than the floor, free VRAM is still
/// admitted. The grant carries the floor to the worker.
#[test]
fn an_apus_ram_floor_is_taken_within_the_cgroup_limit() {
    const LIMIT: u64 = 16 * 1024;
    const VRAM_FREE: u64 = 400;
    // (budget, deliverable RAM, headroom)
    for (budget, ram, headroom) in [
        (VramBudget::default(), 12 * 1024, 10_640),
        (user_margin(0.10), 12 * 1024, 10_640),
        (VramBudget::default(), 1024, VRAM_FREE),
    ] {
        let ledger = apu_host(vec![apu_device(0)], LIMIT, budget);
        let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
        let admission = ledger
            .register_worker("g/apu", item_cost(4), &handle, None)
            .expect("admitted on the APU");
        push_apu(&handle, VRAM_FREE, 60 * 1024, ram, 0);
        ledger.ingest_all_for_test();
        let health = ledger.health();
        let apu = device_of(&health, AMD_A);
        let label = format!("{:?}, {ram} MiB", budget.margin);
        let external = APU_CARVEOUT_MB + LIMIT - (VRAM_FREE + ram) - 1_000;
        assert_eq!(apu.external_mb, external, "{label}");
        assert_eq!(
            (apu.reserve_mb, apu.reserve_rule.as_str()),
            (cpu::ram_reserve_mb(LIMIT), RESERVE_RULE_RAM_FLOOR),
            "{label}"
        );
        assert_eq!(apu.headroom_mb, headroom, "{label}");
        let grant = admission.request_grant(64, None, 1, 0).expect("granted");
        assert_eq!(grant.grant().ram_reserve_mb, apu.reserve_mb, "{label}");
    }
}

/// An APU's load is refused against the smaller of its total and its
/// carve-out plus host RAM less the RAM floor: with GTT raised past that RAM,
/// a base that fits the total but not the RAM is refused.
#[tokio::test]
async fn an_apus_load_is_refused_inside_the_ram_floor() {
    const RAM: u64 = 128 * 1024 - APU_CARVEOUT_MB;
    let raised = || crate::inferio::gpu::GpuInfo {
        total_mb: APU_CARVEOUT_MB + 124 * 1024,
        ..apu_device(0)
    };
    let below_floor = APU_CARVEOUT_MB + RAM - cpu::ram_reserve_mb(RAM);
    // (device, CPU total, room)
    for (device, cpu_total, room) in [
        (raised(), RAM, below_floor),
        (apu_device(0), RAM, APU_TOTAL_MB),
        (apu_device(0), 16 * 1024, APU_CARVEOUT_MB + 14 * 1024),
    ] {
        let total = device.total_mb;
        let ledger = apu_host(vec![device], cpu_total, VramBudget::default());
        assert_eq!(
            ledger.refusal_room_locked(&ledger.lock(), AMD_A),
            room,
            "a {total} MiB APU beside {cpu_total} MiB of RAM"
        );
    }
    assert_eq!(below_floor, 118_016);
    let ledger = apu_host(vec![raised()], RAM, VramBudget::default());
    let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_CARVEOUT_MB + 124 * 1024));
    handle
        .lock()
        .unwrap()
        .load
        .as_mut()
        .expect("a load report")
        .value
        .base_mb = Some(120_000);
    drop(
        ledger
            .register_worker("g/apu", item_cost(4), &handle, None)
            .expect("registers"),
    );
    let Err(refusal) = ledger
        .reserve_load("g/apu", item_cost(4), AMD_A, None)
        .await
    else {
        panic!("a base that only fits inside the RAM floor is refused");
    };
    assert_eq!((refusal.needs_mb, refusal.room_mb), (120_000, below_floor));
}

/// A grant on an APU, or on the CPU device beside one, reads the RAM they
/// share first: the last reading may predate memory the other kept after its
/// own grant settled.
#[test]
fn an_apu_grant_reads_its_ram_first() {
    const RAM: u64 = 128 * 1024 - APU_CARVEOUT_MB;
    let taken = 20 * 1024;
    for device in [AMD_A, cpu::DEVICE_KEY] {
        let ledger = apu_host(vec![apu_device(0)], RAM, VramBudget::default());
        let apu_handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
        let on_apu = ledger
            .register_worker("g/apu", item_cost(4), &apu_handle, None)
            .expect("admitted on the APU");
        let cpu_handle = loaded_on_cpu(Some(RAM));
        let on_cpu = ledger
            .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
            .expect("admitted on RAM");
        push_apu(&apu_handle, 0, 60 * 1024, 40 * 1024, 0);
        push_memory_with_total(&cpu_handle, 40 * 1024, 0, Some(RAM), "ram");
        ledger.ingest_all_for_test();
        let stale = ledger.headroom_mb(device);
        let (total_mb, gtt, admission) = if device == AMD_A {
            let gtt = crate::inferio::gpu::GttBasis {
                gtt_free_mb: 60 * 1024,
                ram_available_mb: 40 * 1024 - taken,
            };
            (APU_TOTAL_MB, Some(gtt), &on_apu)
        } else {
            (RAM, None, &on_cpu)
        };
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: device.to_owned(),
            total_mb,
            free_mb: 40 * 1024 - taken,
            gtt,
        }]));
        let charges = || device_of(&ledger.health(), device).charges_mb;
        let before = charges();
        let _grant = admission.request_grant(64, None, 1, 0).expect("granted");
        assert_eq!(ledger.probe_calls(), 1, "{device}");
        assert_eq!(
            ledger.headroom_mb(device) + (charges() - before),
            stale - taken,
            "{device}"
        );
    }
}

/// An APU's memory is in host RAM only beyond the carve-out it can still
/// use: its footprint plus the VRAM its reading has free. A 40 GiB pool in a
/// 96 GiB carve-out leaves the CPU device's headroom as it was, and so does a
/// 1 000 MiB base beside 15 GiB of other processes' VRAM in a 16 GiB one. A
/// grant on the APU then costs the CPU device what it adds beyond that free
/// VRAM.
#[test]
fn an_apu_counts_in_host_ram_beyond_the_carve_out_it_can_use() {
    // (carve-out, GTT, MemTotal, others' VRAM, pool, deliverable RAM)
    for (carveout, gtt, mem_total, others, pool, ram) in [
        (96 * 1024, 16 * 1024, 32 * 1024, 0, 40 * 1024, 26 * 1024),
        (16 * 1024, 32 * 1024, 64 * 1024, 15 * 1024, 0, 50 * 1024),
    ] {
        let apu = crate::inferio::gpu::GpuInfo {
            total_mb: carveout + gtt,
            unified_ram_mb: Some(carveout + mem_total),
            vram_carveout_mb: Some(carveout),
            ..apu_device(0)
        };
        let ledger = apu_host(vec![apu.clone()], mem_total, VramBudget::default());
        ledger.record_free_for_test(cpu::DEVICE_KEY, ram);
        let before = ledger.headroom_mb(cpu::DEVICE_KEY);
        assert!(before > 0);

        let handle = loaded_rocm(Some("0000:03:00.0"), Some(apu.total_mb));
        let on_apu = ledger
            .register_worker("g/apu", item_cost(4), &handle, None)
            .expect("admitted on the APU");
        // Without the GTT terms (a torch reading) all of the APU's charge
        // comes off the CPU device's room.
        push_pool(&handle, ram, pool, pool, "torch");
        ledger.ingest_all_for_test();
        let charges = || device_of(&ledger.health(), AMD_A).charges_mb;
        let health = ledger.health();
        let row = device_of(&health, cpu::DEVICE_KEY);
        let left = (row.total_mb - row.external_mb - row.reserve_mb).saturating_sub(charges());
        assert_eq!(row.headroom_mb, left, "{carveout}");

        let vram_free = carveout - others - 1_000 - pool;
        push_apu(&handle, vram_free, gtt, ram, pool);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(cpu::DEVICE_KEY), before, "{carveout}");

        let charged = charges();
        let _grant = on_apu.request_grant(64, None, 1, 0).expect("granted");
        let added = charges() - charged;
        assert_eq!(
            before - ledger.headroom_mb(cpu::DEVICE_KEY),
            added.saturating_sub(vram_free),
            "{carveout}"
        );
    }
}

/// The GPU and CPU devices of a Mac or an APU host share its RAM, so a CPU
/// replica counts as a replica on the GPU: a pre-fit GPU grant reserves half
/// the headroom and the CPU replica's window is still priced.
#[test]
fn a_pre_fit_unified_gpu_grant_leaves_ram_for_the_cpu_replica() {
    const RECMAX: u64 = MAC_RAM_MB / 4 * 3;
    const APU_RAM: u64 = MAC_RAM_MB - APU_CARVEOUT_MB;
    for apu in [false, true] {
        let (inventory, gpu, gpu_handle, ram) = if apu {
            let inventory = GpuInventory::known_rocm(vec![apu_device(0)])
                .with_cpu(APU_RAM, crate::inferio::cpu::MemRoots::default());
            let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
            (inventory, AMD_A, handle, APU_RAM)
        } else {
            let inventory = GpuInventory::known_mps(MAC_RAM_MB);
            (inventory, MPS_GPU, loaded_mps(Some(RECMAX)), MAC_RAM_MB)
        };
        let ledger = VramLedger::new(&inventory, no_margin().into(), None);
        ledger.install_probe_stub(None);
        let on_gpu = ledger
            .register_worker("g/gpu", item_cost(4), &gpu_handle, Some(gpu))
            .expect("admitted on the GPU");
        let cpu_handle = loaded_on_cpu(Some(ram));
        let on_cpu = ledger
            .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
            .expect("admitted on RAM");
        // Nothing else holds RAM: both bases are ours, the APU's beyond its
        // carve-out.
        if apu {
            let in_ram = 1_000 - APU_CARVEOUT_MB;
            push_apu(&gpu_handle, 0, 64 * 1024 - in_ram, ram - 1_000 - in_ram, 0);
        } else {
            push_basis(&gpu_handle, RECMAX, MAC_RAM_MB, MAC_RAM_MB - 2_000, 0, 0);
        }
        ledger.ingest_all_for_test();
        let headroom = ledger.headroom_mb(gpu);

        let held = on_gpu.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert_eq!(held.grant().mb, headroom / 2, "{gpu}");
        let other = on_cpu.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert!(other.grant().mb > 0, "{gpu}");
        assert_eq!(other.grant().unit_budget, 4, "{gpu}");
    }
}

/// A load in flight on the CPU device of a Mac counts as a replica on the
/// MPS device too: the MPS replica's pre-fit grant is half of what the
/// load's reservation leaves.
#[tokio::test]
async fn a_load_on_the_cpu_device_of_a_mac_counts_on_the_mps_device() {
    // Metal's three quarters of RAM, under 20 GiB of other usage, so RAM
    // binds.
    const RECMAX: u64 = MAC_RAM_MB / 4 * 3;
    const OTHERS: u64 = 20 * 1024;
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
    let available = MAC_RAM_MB - OTHERS - 1_000;
    push_basis(&mps_handle, RECMAX, MAC_RAM_MB, available, 0, 0);
    ledger.ingest_all_for_test();
    ledger.record_free_for_test(cpu::DEVICE_KEY, available);
    let _loading = ledger
        .reserve_load_for_test("g/cpu", item_cost(4), cpu::DEVICE_KEY, None)
        .await
        .expect("the CPU device");
    let headroom = ledger.headroom_mb(MPS_GPU);
    let reserve = cpu::ram_reserve_mb(MAC_RAM_MB);
    assert_eq!(
        headroom,
        available - reserve - CONSERVATIVE_BASE_MB,
        "the RAM room, below Metal's ceiling"
    );
    assert!(headroom < RECMAX - 1_000);

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
    assert_eq!(
        gpu.reserve_mb,
        cpu::ram_reserve_mb(MAC_RAM_MB),
        "the RAM floor"
    );
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
    ramped_mac_replica_on(mps_ledger())
}

/// [`ramped_mac_replica`] on `ledger`.
fn ramped_mac_replica_on(ledger: Arc<VramLedger>) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
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

/// The RAM reserve of that Mac.
fn mac_reserve() -> u64 {
    cpu::ram_reserve_mb(MAC_RAM_MB)
}

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

/// `windows` windows while macOS pages at `pressure`: the worker holds
/// 180 MiB of pool above the RAM reserve, which is 8 units, and its last
/// reading, from before the paging, still shows 90 000 MiB available. The
/// host reads none.
fn paging_windows(
    ledger: &Arc<VramLedger>,
    handle: &TelemetryHandle,
    admission: &Admission,
    pressure: mps::MemoryPressure,
    windows: usize,
) {
    push_ram(handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 180, 0);
    ledger.health();
    ledger.set_memory_pressure_for_test(pressure);
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: MPS_GPU.to_owned(),
        total_mb: MAC_RAM_MB,
        free_mb: 0,
        gtt: None,
    }]));
    for window in 0..windows {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let grant = *token.grant();
        assert_eq!((grant.unit_budget, grant.mb), (8, 180));
        assert_eq!(grant.squeezed, window == 0, "cut once, then held there");
        // A warm batch, then collapses whose pool growth past the 0 free
        // would corroborate them without the pressure.
        let collapse = BatchMeasurement {
            throughput_collapse: true,
            ..measurement(8, 0, 180)
        };
        let mut batches = vec![measurement(8, 0, 180), warm_batch(8, 100.0)];
        batches.extend((2..WINDOW_DEPTH_MULTIPLIER).map(|_| collapse.clone()));
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
    }
}

/// A full window granted before macOS began paging and settled while it
/// pages: one whose batch the paging began under, growing the pool. Its unit
/// budget.
fn window_that_began_paging(
    ledger: &Arc<VramLedger>,
    handle: &TelemetryHandle,
    admission: &Admission,
) -> u64 {
    window_that_began_paging_ran(ledger, handle, admission, |units| {
        measurement(units, 0, 10 * units + 100)
    })
}

/// [`window_that_began_paging`] whose batch is `batch` of its unit budget.
fn window_that_began_paging_ran(
    ledger: &Arc<VramLedger>,
    handle: &TelemetryHandle,
    admission: &Admission,
    batch: fn(u64) -> BatchMeasurement,
) -> u64 {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    ledger.set_paging_rise_for_test(ledger.pressure_read_at_for_test());
    let granted = token.grant().unit_budget;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![batch(granted)]);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

/// A batch that ran inside the pool it held.
fn inside_its_pool(units: u64) -> BatchMeasurement {
    measurement(units, 10 * units + 100, 10 * units + 100)
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
        // RAM free down to the reserve; while paging none, and the reserve
        // comes off the pool.
        let (available, pool) = if pressure.paging() {
            (0, mac_reserve() + 180)
        } else {
            (mac_reserve(), 180)
        };
        push_ram(&handle, MAC_TOTAL_MB, available, pool, 0);
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

/// While the Mac pages, a grant re-reads the host, which leaves nothing
/// available, so it is cut to the pool the replica holds whatever the
/// worker last reported. Those windows earn no ramp step, feed no knee and
/// deflate nothing. At normal the batch grows back from the size it ran at
/// by doubling, not in one jump.
#[test]
fn while_the_mac_pages_a_grant_fits_the_pool_held_and_grows_back_by_doubling() {
    for pressure in [mps::MemoryPressure::Paging, mps::MemoryPressure::Critical] {
        let (ledger, handle, admission) = ramped_mac_replica();
        let (_, _, samples, budget) = ramp_figures(&ledger);
        assert_eq!(budget, 128, "the trial's next size");
        // A pool that holds the 128 units: the working size all the same.
        ledger.set_memory_pressure_for_test(pressure);
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: MPS_GPU.to_owned(),
            total_mb: MAC_RAM_MB,
            free_mb: 0,
            gtt: None,
        }]));
        push_ram(&handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 1_400, 0);
        assert_eq!(ramp_window(&handle, &admission, &MINILM_M3_MAX), 64);
        paging_windows(&ledger, &handle, &admission, pressure, 3);
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
        // A window the queue sized did not fill the size, so it earns no
        // doubling.
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
}

/// A grant while the Mac pages and a probe of it is already in flight is
/// priced against 0 free, not the reading taken before the paging.
#[test]
fn while_the_mac_pages_a_grant_with_a_probe_in_flight_reads_nothing_free() {
    let (ledger, handle, admission) = ramped_mac_replica();
    push_ram(&handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 180, 0);
    ledger.health();
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Paging);
    ledger.install_probe_stub(None);
    {
        let mut state = ledger.lock();
        let gpu = state.gpus.get_mut(MPS_GPU).expect("the Mac");
        gpu.refreshing = true;
        gpu.free_adjusted_at = Some(Instant::now());
    }
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!((token.grant().unit_budget, token.grant().mb), (8, 180));
    assert_eq!(ledger.probe_calls(), 0);
}

/// While the Mac pages, a load re-reads the host too, and is priced against
/// what it reads rather than a reading taken before the paging; with a probe
/// of the device already in flight, against 0 free. Either way it reserves
/// its whole expected base. A worker's reading recorded while it pages is 0.
#[tokio::test]
async fn while_the_mac_pages_a_load_is_priced_from_a_fresh_reading() {
    for (pressure, refreshing, over_headroom, probes) in [
        (mps::MemoryPressure::Normal, false, false, 0),
        (mps::MemoryPressure::Normal, true, false, 0),
        (mps::MemoryPressure::Paging, false, true, 1),
        (mps::MemoryPressure::Paging, true, true, 0),
    ] {
        let ledger = mps_ledger();
        ledger.record_free_for_test(MPS_GPU, 90_000);
        ledger.set_memory_pressure_for_test(pressure);
        ledger
            .lock()
            .gpus
            .get_mut(MPS_GPU)
            .expect("the Mac")
            .refreshing = refreshing;
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: MPS_GPU.to_owned(),
            total_mb: MAC_RAM_MB,
            free_mb: 0,
            gtt: None,
        }]));
        let (_reservation, exceeds) = ledger
            .reserve_load_signalling("g/a", item_cost(4), MPS_GPU, None)
            .await
            .expect("no refusal")
            .expect("a reservation");
        assert_eq!(exceeds, over_headroom, "{pressure:?} {refreshing}");
        assert_eq!(ledger.probe_calls(), probes, "{pressure:?} {refreshing}");
        let reserved: u64 = ledger.lock().gpus[MPS_GPU].load_reservations.values().sum();
        assert_eq!(reserved, CONSERVATIVE_BASE_MB, "the whole expected base");
    }

    for (pressure, recorded) in [
        (mps::MemoryPressure::Paging, 0),
        (mps::MemoryPressure::Normal, 9_000),
    ] {
        let ledger = mps_ledger();
        let handle = loaded_mps(Some(MAC_TOTAL_MB));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        ledger.set_memory_pressure_for_test(pressure);
        push_ram(&handle, MAC_TOTAL_MB, 9_000, 0, 0);
        ledger.health();
        let state = ledger.lock();
        let free = state.gpus[MPS_GPU].free.as_ref().expect("a reading");
        let ram = free.ram.as_ref().expect("its RAM basis");
        assert_eq!((free.free_mb, ram.available_mb), (recorded, recorded));
    }
}

/// A load under memory pressure warns once per model and device per
/// episode, at any level; while macOS pages it does not also warn that it
/// needs more VRAM than the headroom, which reads 0 then. A reading at
/// normal ends the episode.
#[test]
fn a_load_under_memory_pressure_warns_once_per_model_and_episode() {
    use mps::MemoryPressure::{Critical, Normal, Paging, Warning};
    let ledger = VramLedger::new(
        &GpuInventory::known_mps(MAC_RAM_MB),
        no_margin().into(),
        None,
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let warnings = |pressure: mps::MemoryPressure, free_mb: u64, gpu: &str, models: &[&str]| {
        // A host reading at each load.
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: MPS_GPU.to_owned(),
            total_mb: MAC_RAM_MB,
            free_mb,
            gtt: None,
        }]));
        ledger.lock().gpus.get_mut(MPS_GPU).expect("the Mac").free = None;
        ledger.set_memory_pressure_for_test(pressure);
        let logs = captured_logs(|| {
            for model in models {
                let load = ledger.reserve_load_signalling_for_test(model, item_cost(4), gpu, None);
                runtime.block_on(load).expect("a reservation");
            }
        });
        logs.iter()
            .filter(|(level, _)| *level == tracing::Level::WARN)
            .count()
    };
    let mps_loads = ["g/a", "g/a", "g/a", "g/b"];
    assert_eq!(warnings(Paging, 0, MPS_GPU, &mps_loads), 2);
    assert_eq!(
        warnings(Paging, 0, cpu::DEVICE_KEY, &["g/a"]),
        1,
        "another device"
    );
    assert_eq!(
        warnings(Warning, 90_000, MPS_GPU, &["g/a"]),
        0,
        "the same episode"
    );
    assert_eq!(warnings(Normal, 90_000, MPS_GPU, &["g/a"]), 0);
    assert_eq!(
        warnings(Normal, 0, MPS_GPU, &["g/a"]),
        1,
        "over the headroom"
    );
    assert_eq!(
        warnings(Warning, 90_000, MPS_GPU, &["g/a", "g/a", "g/a"]),
        1,
        "a new episode"
    );
    assert_eq!(
        warnings(Warning, 0, MPS_GPU, &["g/a"]),
        1,
        "over the headroom at warning"
    );
    assert_eq!(warnings(Critical, 0, MPS_GPU, &["g/a"]), 0);
}

/// While macOS pages, a grant that paging cuts warns once per model and
/// device per episode, though its load logged the warning-level line in the
/// same episode; a grant the queue sized does not. A reading at normal ends
/// the episode, and in the next one the cut the first left warns again.
#[test]
fn a_grant_paging_cuts_warns_once_per_model_and_episode() {
    use mps::MemoryPressure::{Normal, Paging, Warning};
    let (ledger, handle, admission) = ramped_mac_replica();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let warnings = |body: &dyn Fn()| {
        captured_logs(body)
            .iter()
            .filter(|(level, _)| *level == tracing::Level::WARN)
            .count()
    };
    ledger.set_memory_pressure_for_test(Warning);
    let load = || {
        let load = ledger.reserve_load_signalling_for_test("g/a", item_cost(4), MPS_GPU, None);
        runtime.block_on(load).expect("a reservation");
    };
    assert_eq!(warnings(&load), 1, "the load");
    ledger.set_memory_pressure_for_test(Paging);
    push_ram(&handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 1_400, 0);
    assert_eq!(warnings(&|| clean_window(&admission)), 0, "not cut");
    // Cut to the 180 MiB pool, then to a 100 MiB one.
    let cut_twice = || {
        paging_windows(&ledger, &handle, &admission, Paging, 1);
        push_ram(&handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 100, 0);
        clean_window(&admission);
    };
    assert_eq!(warnings(&cut_twice), 1);
    ledger.set_memory_pressure_for_test(Normal);
    ledger.health();
    ledger.set_memory_pressure_for_test(Paging);
    // A pool that holds the batch size.
    push_ram(&handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 1_400, 0);
    let one_unit = || {
        let token = admission.request_grant(1, None, 1, 0).expect("granted");
        token.finish(WindowOutcome::Responded { oom: None });
    };
    assert_eq!(warnings(&one_unit), 0);
    assert_eq!(warnings(&|| clean_window(&admission)), 1, "a new episode");
}

/// At warning, once the paging has stopped, the batch grows back by doubling
/// to half the size our batch ran at when the paging began, and no further;
/// each further episode our batch began at the bound, growing the pool past
/// the largest one the episode held, halves it again. The full size returns
/// only at normal.
#[test]
fn at_warning_after_paging_the_batch_regrows_to_half_the_size_paging_began_at() {
    use mps::MemoryPressure::{Normal, Paging, Warning};
    let (ledger, handle, admission) = ramped_mac_replica();
    let shown = |ledger: &Arc<VramLedger>| {
        let worker = ledger.health().swap_remove(0).workers.swap_remove(0);
        (worker.pressure_cap_units, worker.pressure_regrow_to_units)
    };
    assert_eq!(window_that_began_paging(&ledger, &handle, &admission), 128);
    paging_windows(&ledger, &handle, &admission, Paging, 2);
    assert_eq!(shown(&ledger), (Some(8), Some(64)));
    ledger.set_memory_pressure_for_test(Warning);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            throughput_collapse: true,
            ..measurement(8, 0, 180)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        shown(&ledger),
        (Some(8), Some(64)),
        "a collapse earns no doubling"
    );
    assert_eq!(
        ramp_windows(&handle, &admission, 5),
        [8, 16, 32, 64, 64],
        "half of the 128 the paging began under"
    );
    let grew_past_1_380 = |units| measurement(units, 1_380, 1_380 + 10 * units);
    assert_eq!(
        window_that_began_paging_ran(&ledger, &handle, &admission, grew_past_1_380),
        64
    );
    paging_windows(&ledger, &handle, &admission, Paging, 2);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(ramp_windows(&handle, &admission, 4), [8, 16, 32, 32]);
    let grew_past_2_020 = |units| measurement(units, 2_020, 2_020 + 10 * units);
    assert_eq!(
        window_that_began_paging_ran(&ledger, &handle, &admission, grew_past_2_020),
        32
    );
    paging_windows(&ledger, &handle, &admission, Paging, 1);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(ramp_windows(&handle, &admission, 3), [8, 16, 16]);
    assert_eq!(
        ramp_figures(&ledger).0,
        Some(64),
        "no pressure window earned a size"
    );

    ledger.set_memory_pressure_for_test(Normal);
    assert_eq!(ramp_windows(&handle, &admission, 1), [16]);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(ramp_windows(&handle, &admission, 1), [16], "the bound");
    ledger.set_memory_pressure_for_test(Normal);
    assert_eq!(ramp_windows(&handle, &admission, 2), [16, 32]);
    assert_eq!(pressure_cap(&ledger), None, "doubled to what is admitted");
    assert_eq!(ramp_windows(&handle, &admission, 2), [64, 64]);
    assert_eq!(shown(&ledger), (None, None));
}

/// Only an episode our batch began, granted before the paging and growing
/// our pool past the 1 000 MiB the paging windows held, lowers the bound:
/// repeated episodes that began before the grant, or while our batch ran
/// inside the pool it held, leave it, so they never walk the batch down. The
/// bound lasts until the batch is back at what the ramp admits.
#[test]
fn only_an_episode_our_batch_began_lowers_the_bound() {
    use mps::MemoryPressure::{Normal, Paging, Warning};
    let (ledger, handle, admission) = ramped_mac_replica();
    for _ in 0..5 {
        paging_windows(&ledger, &handle, &admission, Paging, 1);
        ledger.set_memory_pressure_for_test(Warning);
        assert_eq!(ramp_windows(&handle, &admission, 4), [8, 16, 32, 64]);
    }
    // Paging at the grant, and 740 MiB of pool above the reserve, which
    // holds the 64 units.
    ledger.set_memory_pressure_for_test(Paging);
    push_ram(&handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 740, 0);
    assert_eq!(ramp_window(&handle, &admission, &MINILM_M3_MAX), 64);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(ramp_windows(&handle, &admission, 2), [64, 64]);

    paging_windows(&ledger, &handle, &admission, Paging, 1);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(ramp_windows(&handle, &admission, 1), [8]);
    assert_eq!(
        window_that_began_paging_ran(&ledger, &handle, &admission, inside_its_pool),
        16
    );
    paging_windows(&ledger, &handle, &admission, Paging, 1);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(
        ramp_windows(&handle, &admission, 4),
        [8, 16, 32, 64],
        "began while our batch ran inside its pool"
    );

    let grew_past_1_000 = |units| measurement(units, 1_000, 1_000 + 10 * units);
    assert_eq!(
        window_that_began_paging_ran(&ledger, &handle, &admission, grew_past_1_000),
        64
    );
    paging_windows(&ledger, &handle, &admission, Paging, 1);
    ledger.set_memory_pressure_for_test(Warning);
    assert_eq!(ramp_windows(&handle, &admission, 4), [8, 16, 32, 32]);
    ledger.set_memory_pressure_for_test(Normal);
    assert_eq!(ramp_windows(&handle, &admission, 2), [32, 64]);
    assert_eq!(pressure_cap(&ledger), None, "back at what the ramp admits");
}

/// A window granted before the paging began that ran a batch at its budget
/// and grew our pool past the largest pool the episode held lowers the bound
/// to at most half that budget, whatever its settle changed; until then the
/// bound is the largest size a paging window asked, whichever settles first.
/// Rows: at warning, a replica that normal pressure would let double its
/// working size, memory having granted nothing above it; a deflation the
/// window's settle repaid; an out-of-memory failure whose middle batch of
/// three grew the pool; a second replica asking less, granted after the
/// paging began and settled first, beside two windows granted before it; a
/// window right after a halving that ran inside its pool; a window whose
/// batches ran below its budget, one collapsing; memory that cut the batch
/// below the bound; a window that refilled a pool released after a paging
/// cut; a batch at the budget whose throughput collapse the pressure
/// suppressed, then one below it.
#[test]
fn paging_our_batch_began_lowers_the_bound_to_half_its_budget() {
    use mps::MemoryPressure::{Paging, Warning};
    type Setup = fn(&Arc<VramLedger>, &TelemetryHandle, &Admission);
    let room_cut: Setup = |ledger, handle, admission| {
        {
            let mut state = ledger.lock();
            let cal = state
                .calibration
                .get_mut(&("g/a".to_owned(), MPS_GPU.to_owned()))
                .expect("calibrated");
            cal.trial = None;
            cal.room_cut = true;
        }
        assert_eq!(ramp_figures(ledger).3, 128, "twice the working size");
        ledger.set_memory_pressure_for_test(Warning);
        assert_eq!(ramp_window(handle, admission, &MINILM_M3_MAX), 64);
        assert_eq!(window_that_began_paging(ledger, handle, admission), 64);
    };
    let deflated: Setup = |ledger, handle, admission| {
        for entry in ledger.lock().workers.values_mut() {
            entry.deflation = 1;
            entry.clean_windows = 2;
        }
        assert_eq!(window_that_began_paging(ledger, handle, admission), 64);
    };
    let out_of_memory: Setup = |ledger, handle, admission| {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        ledger.set_paging_rise_for_test(ledger.pressure_read_at_for_test());
        assert_eq!(token.grant().unit_budget, 128);
        handle.lock().unwrap().record_measurements(vec![
            inside_its_pool(128),
            measurement(128, 0, 1_380),
            inside_its_pool(128),
        ]);
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Marker),
        });
    };
    let two_replicas: Setup = |ledger, handle, admission| {
        let late = loaded_mps(Some(MAC_TOTAL_MB));
        let late_admission = ledger
            .register_worker("g/a", item_cost(4), &late, None)
            .expect("registers");
        ledger.set_memory_pressure_for_test(Warning);
        let early = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let before_halving = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        ledger.set_paging_rise_for_test(ledger.pressure_read_at_for_test());
        assert_eq!(early.grant().unit_budget, 64);
        assert_eq!(before_halving.grant().unit_budget, 64);
        ledger.set_memory_pressure_for_test(Paging);
        // Asks 16.
        ledger
            .lock()
            .workers
            .get_mut(&late_admission.worker_id())
            .expect("the late replica")
            .deflation = 2;
        // A pool above the reserve for 16 units beside the two windows'
        // 1 480 MiB.
        push_ram(&late, MAC_TOTAL_MB, 0, mac_reserve() + 1_740, 0);
        let token = late_admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, 16);
        late.lock()
            .unwrap()
            .record_measurements(vec![measurement(16, 0, 260)]);
        token.finish(WindowOutcome::Responded { oom: None });
        for token in [early, before_halving] {
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![measurement(64, 0, 740)]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        assert_eq!(pressure_cap(ledger).map(|cap| cap.units), Some(16));
    };
    let ran_inside_its_pool: Setup = |ledger, handle, admission| {
        ledger.set_memory_pressure_for_test(Warning);
        assert_eq!(window_that_began_paging(ledger, handle, admission), 64);
        let ran = window_that_began_paging_ran(ledger, handle, admission, inside_its_pool);
        assert_eq!(ran, 32);
    };
    let below_budget: Setup = |ledger, handle, admission| {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        ledger.set_paging_rise_for_test(ledger.pressure_read_at_for_test());
        assert_eq!(token.grant().unit_budget, 128);
        handle.lock().unwrap().record_measurements(vec![
            measurement(64, 0, 740),
            BatchMeasurement {
                throughput_collapse: true,
                ..measurement(64, 740, 1_000)
            },
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
    };
    let below_the_bound: Setup = |ledger, handle, admission| {
        // Free memory for 32 units above the reserve.
        push_ram(handle, MAC_TOTAL_MB, mac_reserve() + 420, 0, 0);
        assert_eq!(window_that_began_paging(ledger, handle, admission), 32);
    };
    let refilled: Setup = |ledger, handle, admission| {
        assert_eq!(window_that_began_paging(ledger, handle, admission), 128);
        paging_windows(ledger, handle, admission, Paging, 1);
        ledger.set_memory_pressure_for_test(Warning);
        assert_eq!(ramp_windows(handle, admission, 3), [8, 16, 32]);
        // From 0 to 740 MiB, below the 1 380 MiB the 128 units held.
        assert_eq!(window_that_began_paging(ledger, handle, admission), 64);
    };
    let collapsed: Setup = |ledger, handle, admission| {
        ledger.set_memory_pressure_for_test(Warning);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        ledger.set_paging_rise_for_test(ledger.pressure_read_at_for_test());
        assert_eq!(token.grant().unit_budget, 64);
        handle.lock().unwrap().record_measurements(vec![
            BatchMeasurement {
                throughput_collapse: true,
                ..measurement(64, 0, 740)
            },
            inside_its_pool(32),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
    };
    for (setup, regrow_to, regrowth) in [
        (room_cut, 32, [8, 16, 32, 32, 32]),
        (deflated, 32, [8, 16, 32, 32, 32]),
        (out_of_memory, 64, [8, 16, 32, 64, 64]),
        (two_replicas, 32, [8, 16, 32, 32, 32]),
        (ran_inside_its_pool, 32, [8, 16, 32, 32, 32]),
        (below_budget, 128, [8, 16, 32, 64, 64]),
        (below_the_bound, 16, [8, 16, 16, 16, 16]),
        (refilled, 64, [8, 16, 32, 64, 64]),
        (collapsed, 32, [8, 16, 32, 32, 32]),
    ] {
        let (ledger, handle, admission) = ramped_mac_replica();
        setup(&ledger, &handle, &admission);
        paging_windows(&ledger, &handle, &admission, Paging, 1);
        assert_eq!(
            pressure_cap(&ledger).map(|cap| cap.regrow_to),
            Some(regrow_to)
        );
        ledger.set_memory_pressure_for_test(Warning);
        assert_eq!(ramp_windows(&handle, &admission, 5), regrowth);
    }
}

/// A Mac CPU replica ramped 4 → 64 as [`ramped_mac_replica`] is.
fn ramped_mac_cpu_replica() -> (Arc<VramLedger>, TelemetryHandle, Admission) {
    let ledger = VramLedger::new(
        &GpuInventory::known_mps(MAC_RAM_MB),
        no_margin().into(),
        None,
    );
    ledger.install_probe_stub(None);
    let handle = loaded_on_cpu(Some(MAC_RAM_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, Some(cpu::DEVICE_KEY))
        .expect("registers");
    push_memory_with_total(&handle, 90_000, 0, Some(MAC_RAM_MB), "ram");
    let ramped: Vec<u64> = (0..6)
        .map(|_| ramp_window(&handle, &admission, &MINILM_M3_MAX))
        .collect();
    assert_eq!(ramped, [4, 4, 8, 16, 32, 64]);
    (ledger, handle, admission)
}

/// On the CPU device, whose pool figure is the peak resident set since
/// start, a window granted before the paging that ran a batch at its budget
/// lowers the bound when that budget was at least the smaller of the bound
/// and the size asked, and it was granted after the last window our batch
/// began.
#[test]
fn on_the_cpu_device_a_window_at_the_bound_lowers_it() {
    let regrow_to = |ledger: &Arc<VramLedger>| {
        let key = ("g/a".to_owned(), cpu::DEVICE_KEY.to_owned());
        ledger.lock().calibration[&key]
            .pressure_cap
            .map(|cap| cap.regrow_to)
    };
    // A batch that left the peak resident set where it was.
    let ran = |handle: &TelemetryHandle, token: GrantToken| {
        let units = token.grant().unit_budget;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                reserved_before_mb: Some(5_000),
                reserved_after_mb: Some(5_000),
                ..measurement(units, 0, 10 * units + 100)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
    };

    let (ledger, handle, admission) = ramped_mac_cpu_replica();
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Warning);
    let at_the_bound = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    // Asks 32.
    for entry in ledger.lock().workers.values_mut() {
        entry.deflation = 1;
    }
    let before_halving = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    ledger.set_paging_rise_for_test(ledger.pressure_read_at_for_test());
    assert_eq!(at_the_bound.grant().unit_budget, 64);
    assert_eq!(before_halving.grant().unit_budget, 32);
    ran(&handle, at_the_bound);
    assert_eq!(regrow_to(&ledger), Some(32));
    ran(&handle, before_halving);
    assert_eq!(regrow_to(&ledger), Some(32), "granted before it lowered");

    let (ledger, handle, admission) = ramped_mac_cpu_replica();
    // Free memory for 32 units over the host RAM reserve.
    let free = cpu::ram_reserve_mb(MAC_RAM_MB) + 420;
    push_memory_with_total(&handle, free, 0, Some(MAC_RAM_MB), "ram");
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    ledger.set_paging_rise_for_test(ledger.pressure_read_at_for_test());
    assert_eq!(token.grant().unit_budget, 32);
    ran(&handle, token);
    assert_eq!(regrow_to(&ledger), Some(128), "below the 128 asked");
}

/// Halving the bound leaves at least one unit, or a one-unit batch that
/// began the paging would be capped at none and never grow back.
#[test]
fn the_bound_of_a_one_unit_batch_is_one_unit() {
    let ledger = mps_ledger();
    let handle = loaded_mps(Some(MAC_TOTAL_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(1), &handle, None)
        .expect("registers");
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Warning);
    push_ram(&handle, MAC_TOTAL_MB, 0, 0, 0);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    ledger.set_paging_rise_for_test(ledger.pressure_read_at_for_test());
    assert_eq!(token.grant().unit_budget, 1);
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(1, 0, 10)]);
    token.finish(WindowOutcome::Responded { oom: None });
    let cap = pressure_cap(&ledger).expect("a cap");
    assert_eq!(
        (cap.units, cap.regrow_to, cap.halved_at.is_some()),
        (1, 1, true)
    );
}

/// Paging that began as the grant read the pressure counts at settle: the
/// window is a paging window. Paging from before the grant does not.
#[test]
fn paging_that_began_at_the_grants_own_reading_counts_at_settle() {
    for before_grant in [false, true] {
        let ledger = mps_ledger();
        let handle = loaded_mps(Some(MAC_TOTAL_MB));
        let admission = ledger
            .register_worker("g/a", item_cost(1), &handle, None)
            .expect("registers");
        push_ram(&handle, MAC_TOTAL_MB, 0, 0, 0);
        let before = Instant::now() - Duration::from_millis(1);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        ledger.set_paging_rise_for_test(if before_grant {
            before
        } else {
            ledger.pressure_read_at_for_test()
        });
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(1, 0, 10)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(pressure_cap(&ledger).is_some(), !before_grant);
    }
}

/// The size paging left belongs to the model on the device, so a replica
/// loaded afterwards runs what the one that lived through it runs.
#[test]
fn a_reloaded_replica_inherits_the_size_paging_left() {
    let (ledger, handle, admission) = ramped_mac_replica();
    paging_windows(&ledger, &handle, &admission, mps::MemoryPressure::Paging, 1);
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
    push_ram(&handle, MAC_TOTAL_MB, 0, mac_reserve() + 180, 0);
    queued_window_at_the_rate(&handle, &admission, 5, |_| 100.0);
    assert_eq!(pressure_cap(&ledger), None, "5 units of work, a pool for 8");

    let granted = queued_window_at_the_rate(&handle, &admission, 20, |_| 100.0);
    assert_eq!(granted, 8, "20 units of work, memory for 8");
    assert_eq!(pressure_cap(&ledger).map(|cap| cap.units), Some(8));

    ledger.set_knee_for_test("g/a", MPS_GPU, 3);
    assert_eq!(ramp_figures(&ledger).3, 3);
}

/// A pre-fit Mac replica whose first window ran five batches of 8 units
/// allocating 160 MiB, now asked for 16, holding 120 MiB of pool above the
/// RAM reserve while macOS pages.
fn paging_pre_fit_mac_replica() -> (Arc<VramLedger>, TelemetryHandle, Admission) {
    let ledger = mps_ledger();
    let handle = loaded_mps(Some(MAC_TOTAL_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .expect("registers");
    push_ram(&handle, MAC_TOTAL_MB, 90_000, 0, 0);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(8, 0, 160); 5]);
    token.finish(WindowOutcome::Responded { oom: None });
    admission.earn_next_size();
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Paging);
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: MPS_GPU.to_owned(),
        total_mb: MAC_RAM_MB,
        free_mb: 0,
        gtt: None,
    }]));
    push_ram(&handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 120, 0);
    ledger.health();
    (ledger, handle, admission)
}

/// A window with `work` units in the queue whose batches allocate 20 MiB a
/// unit, as [`paging_pre_fit_mac_replica`]'s did; its grant.
fn window_at_20_mib_a_unit(handle: &TelemetryHandle, admission: &Admission, work: u64) -> Grant {
    let token = admission.request_grant(work, None, 1, 0).expect("granted");
    let grant = *token.grant();
    let units = grant.unit_budget;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(units, 0, 20 * units)]);
    token.finish(WindowOutcome::Responded { oom: None });
    grant
}

/// While macOS pages, a pre-fit replica alone on the Mac runs what the pool
/// it holds covers at its pre-fit price, not its batch size: nothing beyond
/// the pool is free. A window there that the queue sized, squeezed or not,
/// sets no size kept. At warning with as little free, the size is held.
#[test]
fn while_the_mac_pages_a_pre_fit_batch_fits_the_pool_held() {
    let (ledger, handle, admission) = paging_pre_fit_mac_replica();
    assert_eq!(
        window_at_20_mib_a_unit(&handle, &admission, u64::MAX).unit_budget,
        6,
        "120 MiB of pool at 20 MiB a unit, not the 16 asked"
    );
    assert_eq!(pressure_cap(&ledger).map(|cap| cap.units), Some(6));

    let (ledger, handle, admission) = paging_pre_fit_mac_replica();
    let grant = window_at_20_mib_a_unit(&handle, &admission, 2);
    assert_eq!((grant.unit_budget, grant.squeezed), (2, true));
    assert_eq!(pressure_cap(&ledger), None, "2 units of work, a pool for 6");

    let (ledger, handle, admission) = paging_pre_fit_mac_replica();
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Warning);
    push_ram(&handle, MAC_TOTAL_MB, 40, 120, 0);
    ledger.health();
    let grant = window_at_20_mib_a_unit(&handle, &admission, u64::MAX);
    assert_eq!(grant.unit_budget, 16, "at warning the batch size is held");
}

/// While macOS pages, a pre-fit batch fits the pool held less the deficit
/// against the reserve, once: 140 MiB above the reserve is 7 units at 20 MiB
/// a unit. A window still out holds 100 MiB of that pool, so the share is
/// only 40 MiB, 2 units; the pool held, not the share, sizes the batch.
#[test]
fn while_the_mac_pages_the_deficit_comes_off_the_pool_held_once() {
    let ledger = mps_ledger();
    let handle = loaded_mps(Some(MAC_TOTAL_MB));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .expect("registers");
    push_ram(&handle, MAC_TOTAL_MB, 90_000, 0, 0);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(8, 0, 160); 5]);
    token.finish(WindowOutcome::Responded { oom: None });
    admission.earn_next_size();
    // 100 MiB above the reserve before macOS pages: the window takes it.
    push_ram(&handle, MAC_TOTAL_MB, mac_reserve() + 100, 0, 0);
    ledger.health();
    let out = admission.request_grant(2, None, 1, 0).expect("granted");
    assert_eq!(out.grant().mb, 100);
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Paging);
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: MPS_GPU.to_owned(),
        total_mb: MAC_RAM_MB,
        free_mb: 0,
        gtt: None,
    }]));
    push_ram(&handle, MAC_TOTAL_MB, 90_000, mac_reserve() + 140, 0);
    ledger.health();
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let grant = token.grant();
    assert_eq!((grant.mb, grant.unit_budget), (40, 7));
}

/// At warning with free RAM below the reserve the grant keeps the pool the
/// replica holds and says macOS is not paging, so the worker's clamp keeps
/// it too (`test_packing.py`, the same figures); while paging it says so.
#[test]
fn at_warning_a_grant_below_the_reserve_keeps_the_pool_and_says_so() {
    let (ledger, handle, admission) = ramped_mac_replica();
    let grant = |pressure| {
        ledger.set_memory_pressure_for_test(pressure);
        push_ram(&handle, MAC_TOTAL_MB, 0, mac_reserve() + 180, 0);
        ledger.health();
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let grant = *token.grant();
        drop(token);
        grant
    };
    let warning = grant(mps::MemoryPressure::Warning);
    assert_eq!(
        (
            warning.unit_budget,
            warning.mb,
            warning.fixed_mb,
            warning.ram_reserve_mb,
            warning.paging
        ),
        (64, 740, 100, 13_107, false)
    );
    assert_eq!(mac_reserve(), 13_107);
    let paging = grant(mps::MemoryPressure::Paging);
    assert_eq!(
        (paging.unit_budget, paging.mb, paging.paging),
        (8, 180, true),
        "the 180 MiB of pool above the reserve"
    );
}

/// At warning with nothing being paged out the replica keeps its working
/// size: the trial of the next one is put off, and there is no growth and
/// no throughput sample. A squeeze there is not kept once its cause is
/// gone, and leaves the replica the pool it holds. The trial is taken up
/// again after the pressure ends, within two doublings of the working size.
#[test]
fn at_warning_without_paging_the_batch_size_is_held() {
    let (ledger, handle, admission) = ramped_mac_replica();
    let samples = ramp_figures(&ledger).2;
    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Warning);
    assert_eq!(ramp_figures(&ledger).3, 64);
    assert_eq!(
        ledger.window_target_units(admission.worker_id()),
        64 * WINDOW_DEPTH_MULTIPLIER
    );
    let held: Vec<u64> = (0..3)
        .map(|_| ramp_window(&handle, &admission, &MINILM_M3_MAX))
        .collect();
    assert_eq!(held, [64, 64, 64], "the trial under way is put off");
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
    push_ram(&handle, MAC_TOTAL_MB, mac_reserve(), 180, 0);
    assert_eq!(ramp_window(&handle, &admission, &MINILM_M3_MAX), 8);
    assert_eq!(
        pressure_cap(&ledger),
        None,
        "a squeeze, not a paging episode"
    );
    assert_eq!(ramp_windows(&handle, &admission, 1), [64]);
    // Under the default margin free memory is short of the reserve, which
    // does not come out of the 740 MiB pool held.
    let (held, held_handle, held_admission) =
        ramped_mac_replica_on(mps_ledger_with(VramBudget::default()));
    held.set_memory_pressure_for_test(mps::MemoryPressure::Warning);
    push_ram(&held_handle, MAC_TOTAL_MB, 300, 740, 0);
    assert_eq!(
        ramp_window(&held_handle, &held_admission, &MINILM_M3_MAX),
        64
    );
    held.set_memory_pressure_for_test(mps::MemoryPressure::Paging);
    assert_eq!(
        ramp_window(&held_handle, &held_admission, &MINILM_M3_MAX),
        1,
        "while paging it does"
    );

    ledger.set_memory_pressure_for_test(mps::MemoryPressure::Normal);
    let after = ramp_windows(&handle, &admission, RETEST_WINDOWS as usize + 3);
    assert_eq!(
        after[..RETEST_WINDOWS as usize],
        [64; RETEST_WINDOWS as usize]
    );
    assert_eq!(after[RETEST_WINDOWS as usize..], [128, 256, 256]);
}

/// Pressure at either end of a window marks it: at the grant only, or at the
/// settle only. It earns no step and puts off the trial under way.
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
        let granted = token.grant().unit_budget;
        let rate = ladder_rate(&MINILM_M3_MAX, granted);
        let mut batches = vec![BatchMeasurement {
            duration_ms: Some(granted as f64 * 1000.0 / rate),
            ..measurement(granted, 0, 10 * granted + 100)
        }];
        batches.extend((1..WINDOW_DEPTH_MULTIPLIER).map(|_| warm_batch(granted, rate)));
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        let ends = format!("{at_grant:?} at the grant, {at_settle:?} at the settle");
        assert_eq!(
            ramp_figures(&ledger).0,
            Some(64),
            "{granted} units earned nothing: {ends}"
        );
        assert_eq!(ledger.trial_for_test("g/a", MPS_GPU).0, None, "{ends}");
    }
}

/// The cap a death left and the cap paging left bound the batch together,
/// the smaller ruling, and each ends on its own terms. A worker that dies
/// while the Mac pages sets the death cap like any other death; the paging
/// cap lifts once the batch has grown back, the death cap stays.
#[test]
fn a_death_cap_and_a_paging_cap_hold_the_smaller_batch() {
    let (ledger, handle, admission) = ramped_mac_replica();
    paging_windows(&ledger, &handle, &admission, mps::MemoryPressure::Paging, 2);
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
