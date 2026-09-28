use super::*;

/// Per-batch free: every measurement's `free_mb` refreshes the
/// GPU, so `external_mb` follows the world at **response** cadence instead of at
/// the window boundary.
#[test]
fn every_batchs_free_reading_refreshes_the_gpus_external_usage() {
    let ledger = ledger(32_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].external_mb, 1_000);

    // One window of three batches, during which something else takes 20 GB and then
    // gives half of it back.
    handle.lock().unwrap().memory = None;
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle.lock().unwrap().record_measurements(vec![
        measurement_with_free(4, 0, 10, 30_000, "nvml"),
        measurement_with_free(4, 10, 20, 10_000, "nvml"),
        measurement_with_free(4, 20, 30, 20_000, "nvml"),
    ]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].external_mb,
        32_000 - 20_000 - 1_030,
        "the last measurement of the response is the freshest reading in it, \
         and our own footprint against it is that batch's pool (1000 base \
         + 30) rather than the one from before the window"
    );
}

/// The run2/S9 soak signature, reproduced and then closed.
///
/// Two residents on one GPU. A holds a grant and grows its pool through a
/// long window while B's replies keep the device-wide free reading fresh.
/// Before A's pool figure is refreshed, A's growth is booked as another
/// process's memory: `external` rises by it, `limit` collapses, `headroom`
/// pins at 0, and the same MB is subtracted twice — once as external, once
/// as A's own charge — so `external + charges` exceeds the whole card.
/// After A's per-batch memory frame lands in its telemetry, none of that
/// happens: `external` reads what the *hog* holds, and `headroom` stays
/// positive.
#[test]
fn an_in_flight_replicas_pool_growth_is_not_another_processs_memory() {
    const TOTAL: u64 = 100_000;
    let ledger = ledger(TOTAL, no_margin());
    let big = loaded(Some(1_000), Some(0));
    let small = loaded(Some(500), Some(0));
    let a = ledger
        .register_worker("g/big", item_cost(4), &big, None)
        .expect("registers");
    let b = ledger
        .register_worker("g/small", item_cost(4), &small, None)
        .expect("registers");
    // A quiet card: 500 MB of somebody else's, our two bases, nothing more.
    push_memory_with_total(&big, TOTAL - 1_000 - 500 - 500, 0, Some(TOTAL), "nvml");
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].external_mb, 500, "the hog, and only it");

    // A takes a grant and starts a long window. Its pool climbs to 52 GB —
    // the soak's median grant — and the card's free reading falls with it,
    // but nothing of A's reaches the ledger until its reply.
    big.lock().unwrap().memory = None;
    let window = a.request_grant(u64::MAX, None, 1, 0).expect("granted");
    const GROWTH: u64 = 52_000;
    let free_now = TOTAL - 1_000 - 500 - 500 - GROWTH;

    // B settles a window of its own, which is what refreshes `free`.
    let neighbour = b.request_grant(u64::MAX, None, 1, 0).expect("granted");
    small
        .lock()
        .unwrap()
        .record_measurements(vec![measurement_with_free(4, 0, 0, free_now, "nvml")]);
    neighbour.finish(WindowOutcome::Responded { oom: None });

    // The defect, in the four figures the soak reported it in.
    let before = &ledger.health()[0];
    assert_eq!(
        before.external_mb,
        500 + GROWTH,
        "S4: /health books our own in-flight pool as somebody else's"
    );
    assert_eq!(
        before.footprints_mb, 1_500,
        "A's pool is from before the window"
    );
    assert_eq!(before.headroom_mb, 0, "S3: admission stalls against it");
    assert!(
        before.external_mb + before.charges_mb > TOTAL,
        "S2: the granted pool is subtracted twice — {} + {} > {TOTAL}",
        before.external_mb,
        before.charges_mb
    );

    // The per-batch memory frame: A's pool as of its last batch, with the
    // free reading taken beside it.
    push_memory_with_total(&big, free_now, GROWTH, Some(TOTAL), "nvml");

    let after = &ledger.health()[0];
    assert_eq!(after.external_mb, 500, "the hog, and only it, again");
    assert_eq!(
        after.footprints_mb,
        1_500 + GROWTH,
        "A's growth is charged to A"
    );
    assert!(after.headroom_mb > 0, "S3: admission is not stalled");
    assert!(
        after.external_mb + after.charges_mb <= TOTAL,
        "S2: nothing is subtracted twice — {} + {} <= {TOTAL}",
        after.external_mb,
        after.charges_mb
    );
    // And the grant it is spending is unchanged by any of this: the fix is
    // about what the memory is *called*, not about what was handed out.
    assert_eq!(after.grants_outstanding, 1);
    window.finish(WindowOutcome::Responded { oom: None });
}

/// The pull is freshness-guarded, as the trim path's is: a sample older
/// than the pool figure already charged is not a newer reading of it, and
/// the departed-replica credit still stands in front of the free half.
#[test]
fn a_stale_frame_never_overwrites_a_newer_pool_reading() {
    let ledger = ledger(32_000, no_margin());
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_memory_with_total(&handle, 25_000, 4_000, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].footprints_mb, 5_000);

    // A sample captured before the one already charged: the pool figure
    // holds, and so does the free reading it arrived with.
    {
        let mut telemetry = handle.lock().unwrap();
        let older = telemetry
            .memory
            .as_ref()
            .expect("a sample is charged")
            .captured_at
            - Duration::from_secs(5);
        telemetry.memory = Some(Timestamped {
            value: MemorySample {
                free_mb: Some(31_000),
                total_mb: Some(32_000),
                free_source: Some("nvml".to_owned()),
                reserved_mb: Some(0),
                allocated_mb: Some(0),
                ..MemorySample::default()
            },
            captured_at: older,
        });
    }
    let health = &ledger.health()[0];
    assert_eq!(
        health.footprints_mb, 5_000,
        "the older pool figure is refused"
    );
    assert_eq!(health.external_mb, 32_000 - 25_000 - 5_000);

    // A newer one lands, both halves.
    push_memory_with_total(&handle, 20_000, 9_000, Some(32_000), "nvml");
    let health = &ledger.health()[0];
    assert_eq!(health.footprints_mb, 10_000);
    assert_eq!(health.external_mb, 32_000 - 20_000 - 10_000);
    drop(admission);
}

/// Within one response, our own pool is contemporaneous with the free
/// readings it is netted against: a reply that carried measurements but no
/// response-level `memory` map — a worker whose allocator answers and whose
/// driver does not — still advances this replica's pool from the batches'
/// own `peak_reserved`.
#[test]
fn a_windows_batches_carry_its_pool_when_the_reply_carries_none() {
    let ledger = ledger(32_000, no_margin());
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].external_mb, 1_000);

    handle.lock().unwrap().memory = None;
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    handle.lock().unwrap().record_measurements(vec![
        measurement_with_free(4, 0, 500, 29_000, "nvml"),
        measurement_with_free(4, 500, 1_500, 28_000, "nvml"),
    ]);
    token.finish(WindowOutcome::Responded { oom: None });

    let health = &ledger.health()[0];
    assert_eq!(
        health.footprints_mb, 2_500,
        "1000 base + the 1500 it grew to"
    );
    assert_eq!(
        health.external_mb,
        32_000 - 28_000 - 2_500,
        "our own growth comes out of external, not out of the hog"
    );
    drop(admission);
}

/// A per-batch memory frame is applied when it **arrives**, not when the
/// window settles: mid-window it moves `external_mb` and the limit the next
/// grant is priced against, and it obeys the currency check on the way in.
/// What waits for the settle is the fit — the frame is telemetry and moves
/// no measurement watermark.
#[test]
fn a_mid_window_frame_moves_the_next_grants_price_before_the_settle() {
    const TOTAL: u64 = 100_000;
    let ledger = ledger(TOTAL, no_margin());
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory_with_total(&handle, 98_000, 0, Some(TOTAL), "nvml");
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].external_mb, 1_000);
    let before = ledger.health()[0].limit_mb;

    // A long window opens, and a neighbouring process takes 30 GB inside it.
    let window = admission.request_grant(64, None, 1, 0).expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(64, 0, 740)]);
    // A frame whose own total describes some other device is in a different
    // currency and is refused, exactly as a response-level sample is.
    push_memory_with_total(&handle, 68_000, 0, Some(8_192), "nvml");
    assert_eq!(ledger.health()[0].external_mb, 1_000, "wrong currency");

    push_memory_with_total(&handle, 68_000, 0, Some(TOTAL), "nvml");
    assert_eq!(
        ledger.health()[0].external_mb,
        31_000,
        "the step is visible one batch after it happened, not one window"
    );
    assert_eq!(
        ledger.health()[0].limit_mb,
        before - 30_000,
        "and the next grant is priced against it"
    );
    assert_eq!(
        fit_sample_count(&ledger),
        0,
        "while the fit still waits for the window to settle"
    );

    window.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        fit_sample_count(&ledger),
        1,
        "the settle path fits the same sample it always did"
    );
}

/// The staleness clock is read **after** the frames are folded in, so a
/// load priced while a resident is mid-window is not made to wait on a host
/// driver query for a number a frame already carried.
#[tokio::test]
async fn a_frame_fresh_gpu_is_not_re_probed_before_a_load() {
    let ledger = ledger(32_000, no_margin());
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 32_000,
        free_mb: 1_000,
    }]));
    let handle = loaded(Some(1_000), Some(0));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    // The GPU's own reading is old enough to be due a probe; the frame
    // sitting in the resident's telemetry is not.
    ledger.lock().gpus.get_mut(GPU).expect("the GPU").free = Some(FreeSample {
        free_mb: 20_000,
        source: "nvml".to_owned(),
        at: Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1),
        ram: None,
    });
    push_memory_with_total(&handle, 25_000, 0, Some(32_000), "nvml");

    let _reservation = ledger
        .reserve_load_for_test("g/b", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    assert_eq!(ledger.probe_calls(), 0, "the frame already answered it");
    assert_eq!(
        ledger.health()[0].external_mb,
        32_000 - 25_000 - 1_000,
        "and the frame's reading is what the load was priced against"
    );
}

/// The rules the per-batch readings inherit, each shown binding: source precedence,
/// the sample's own total as a currency check, and the departed-replica credit.
#[test]
fn per_batch_free_readings_obey_the_sample_map_rules() {
    let ledger = ledger(32_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    handle.lock().unwrap().memory = None;

    // A `torch` reading on a GPU that has seen NVML: dropped, exactly as a torch
    // sample-map reading is.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement_with_free(4, 0, 10, 5_000, "torch")]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].external_mb,
        990,
        "the free reading is unmoved; what moved is our own footprint, by \
         the 10 MB of pool the batch reported, which comes out of external"
    );

    // An authoritative reading whose response claims a total that does not
    // describe this GPU is in a different currency, and is refused with
    // the response-level sample it arrived beside.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    {
        let mut telemetry = handle.lock().unwrap();
        telemetry.memory = Some(Timestamped::now(MemorySample {
            free_mb: Some(6_000),
            total_mb: Some(8_192),
            free_source: Some("nvml".to_owned()),
            reserved_mb: Some(0),
            allocated_mb: Some(0),
            ..MemorySample::default()
        }));
        telemetry.record_measurements(vec![measurement_with_free(4, 0, 10, 6_000, "nvml")]);
    }
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].external_mb,
        1_000,
        "a reading of some other GPU is not a reading of this one; its \
         response-level sample still states our own pool, and states it 0"
    );

    // And an honest one lands.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    {
        let mut telemetry = handle.lock().unwrap();
        telemetry.memory = None;
        telemetry.record_measurements(vec![measurement_with_free(4, 0, 10, 25_000, "nvml")]);
    }
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].external_mb, 32_000 - 25_000 - 1_010);
}

/// A window that ended in an OOM still refreshes the GPU: the reading
/// describes the GPU, not the batch's outcome, and it is precisely the
/// moment the freshest picture is worth most.
#[test]
fn a_negative_windows_free_readings_still_reach_the_gpu() {
    let ledger = ledger(32_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    handle.lock().unwrap().memory = None;

    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            ..measurement_with_free(4, 0, 10, 2_000, "nvml")
        }]);
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    assert_eq!(
        ledger.health()[0].external_mb,
        32_000 - 2_000 - 1_010,
        "the GPU is nearly full, which is what the OOM was about"
    );
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
}

/// `external` is clamped at 0: `free` and the per-worker samples come
/// from different moments, so skew must never manufacture phantom
/// headroom (an unclamped subtraction would go negative here).
#[test]
fn external_clamps_at_zero() {
    let ledger = ledger(10_000, VramBudget::default());
    let handle = loaded(Some(8000), Some(0));
    let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
    // free 9000 + our 8000 > total 10000 — impossible in one instant.
    push_memory(&handle, 9000, 0);
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.external_mb, 0, "clamped, never negative");
    assert_eq!(gpu.limit_mb, 10_000, "no external usage to margin");
    assert_eq!(gpu.headroom_mb, 2000, "10000 - 8000 footprint");
}

/// A worker with no reported base (CTranslate2, a remote API behind a
/// torch import) contributes only pool growth; its real VRAM lands in
/// `external`, which is the intended accounting, not phantom headroom.
#[test]
fn a_baseless_worker_contributes_only_pool_growth() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(None, Some(0));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 4000, 300);
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.footprints_mb, 300, "pool growth only, no base");
    assert_eq!(gpu.external_mb, 5700, "everything else is external");
}

/// The dispatch path folds the per-batch frames in **before** it prices a
/// window, so a reading that arrived mid-window is what the next grant is
/// sized against — one pass, ahead of the staleness clock the probe reads.
#[test]
fn a_frame_that_arrived_mid_window_prices_the_next_grant() {
    const TOTAL: u64 = 32_000;
    let ledger = ledger(TOTAL, no_margin());
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    // The ledger's own reading has the GPU nearly full; the frame in the
    // resident's telemetry says 25 GB came back.
    ledger.lock().gpus.get_mut(GPU).expect("the GPU").free = Some(FreeSample {
        free_mb: 2_000,
        source: "nvml".to_owned(),
        at: Instant::now(),
        ram: None,
    });
    push_memory_with_total(&handle, 25_000, 0, Some(TOTAL), "nvml");

    // Priced before anything else reads the ledger, so only `request_grant`'s
    // own fold-in can have applied the frame.
    let grant = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        grant.grant().mb,
        24_100,
        "priced against the frame's 25 GB, not the ledger's own 2 GB"
    );
}

/// The worker's `"ram"` samples are **authoritative**: they are the OS's
/// own whole-machine statistics, and on this backend they are the only
/// reading there is, so external pressure has to be derived from them.
#[test]
fn a_ram_sample_prices_external_pressure() {
    let ledger = cpu_ledger(no_margin());
    let handle = loaded_cpu(Some(CPU_RAM_MB));
    let _admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    // A browser eating most of the machine shows up exactly the way a
    // game eating VRAM does on a dGPU.
    push_memory_with_total(&handle, 8_192, 0, Some(CPU_RAM_MB), "ram");
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert!(gpu.external_known);
    assert_eq!(
        gpu.external_mb,
        CPU_RAM_MB - 8_192 - 1000,
        "total − free − our own base"
    );
}

/// The load response's memory sample is the only reading a fresh GPU has.
#[test]
fn the_load_report_seeds_the_gpus_free_reading() {
    let ledger = ledger(32_768, no_margin());
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1024),
        reserved_at_load_mb: Some(0),
        allocated_at_load_mb: Some(0),
        gpu_uuid: Some(GPU.to_owned()),
        memory: Some(MemorySample {
            // 20 GB is held by something else; only ~11 GB is free.
            free_mb: Some(11_264),
            total_mb: Some(32_768),
            free_source: Some("nvml".to_owned()),
            reserved_mb: Some(0),
            allocated_mb: Some(0),
            ..MemorySample::default()
        }),
        ..LoadReport::default()
    }));
    let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    let gpu = &ledger.health()[0];
    assert!(gpu.external_known, "the load report is a reading");
    assert_eq!(
        gpu.external_mb, 20_480,
        "32768 total - 11264 free - 1024 ours"
    );
    assert_eq!(gpu.limit_mb, 32_768 - 20_480);
    // And the very first grant is priced against it.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert!(
        token.grant().mb <= 32_768 - 20_480,
        "the first window does not get the whole card: {:?}",
        token.grant()
    );
}

/// Source precedence: a whole-GPU reading outranks a context-scoped one and
/// is never overwritten by it, on both backends that have an authoritative
/// source. `mem_get_info` describes one CUDA context and reads gigabytes
/// apart from NVML's whole-GPU figure, so alternating them would swing
/// `external` — and every grant — for no physical reason.
#[test]
fn a_whole_gpu_reading_outranks_a_torch_one_on_every_backend() {
    assert!(free_source_is_authoritative("nvml"));
    assert!(free_source_is_authoritative("amdgpu-sysfs"));
    assert!(
        !free_source_is_authoritative("sysfs"),
        "a bare sysfs label must not inherit authority"
    );
    assert!(!free_source_is_authoritative("torch"));
    assert_eq!(
        GpuMemoryQuery::RocmSysfs {
            pci_devices: std::path::PathBuf::from("/sys/bus/pci/devices"),
            meminfo: std::path::PathBuf::from("/proc/meminfo"),
            gpus: Vec::new().into(),
        }
        .free_source(),
        "amdgpu-sysfs",
        "the label the refresh actually records under"
    );

    for authoritative in ["nvml", "amdgpu-sysfs"] {
        let ledger = ledger(32_768, no_margin());
        let handle = loaded(Some(1024), Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        let push = |free_mb: u64, source: &str| {
            let mut telemetry = handle.lock().unwrap();
            telemetry.memory = Some(Timestamped::now(MemorySample {
                free_mb: Some(free_mb),
                total_mb: Some(32_768),
                free_source: Some(source.to_owned()),
                reserved_mb: Some(0),
                allocated_mb: Some(0),
                ..MemorySample::default()
            }));
        };

        // Only torch has answered so far, so its reading is used.
        push(28_000, "torch");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_source.as_deref(), Some("torch"));
        let torch_only_limit = ledger.health()[0].limit_mb;

        // The whole-GPU source answers: it wins, and the limit moves with it.
        push(24_500, authoritative);
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.external_source.as_deref(), Some(authoritative));
        let authoritative_limit = gpu.limit_mb;
        assert_ne!(authoritative_limit, torch_only_limit);

        // A later torch reading is still recorded as telemetry, but must not
        // move the GPU's free figure back.
        push(28_000, "torch");
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert_eq!(
            gpu.external_source.as_deref(),
            Some(authoritative),
            "{authoritative} has precedence once it has answered"
        );
        assert_eq!(
            gpu.limit_mb, authoritative_limit,
            "no gigabyte swing on source alone"
        );
    }
}

/// A replica that leaves the GPU must not have its memory reattributed to
/// *external* usage.
#[test]
fn a_departed_replicas_footprint_is_not_reattributed_to_external() {
    let ledger = ledger(32_000, no_margin());
    let handle = loaded(Some(4_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("admitted");
    // 20 GB free with our 4 GB resident on a 32 GB GPU: 8 GB is somebody else's.
    push_memory_with_total(&handle, 20_000, 0, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].external_mb, 8_000, "8 GB is external");

    drop(admission);

    let gpu = &ledger.health()[0];
    assert_eq!(
        gpu.external_mb, 8_000,
        "the departure changed nothing about anyone else's usage"
    );
    assert_eq!(gpu.total_mb - gpu.limit_mb, 8_000, "nor about the limit");
    let state = ledger.lock();
    assert!(
        refresh_due(state.gpus.get(GPU).expect("the GPU")),
        "and the adjusted reading is due a live probe, whatever its age"
    );
}

/// The adjustment is arithmetic standing in for a measurement, so the next
/// real reading overrides it outright — including when the departed memory
/// did *not* come back to the GPU (something else took it meanwhile).
#[test]
fn a_later_free_reading_supersedes_the_departure_adjustment() {
    let ledger = ledger(32_000, no_margin());
    let departing = loaded(Some(4_000), Some(0));
    let staying = loaded(Some(1_000), Some(0));
    let leaving = ledger
        .register_worker("g/a", item_cost(4), &departing, None)
        .expect("admitted");
    let _resident = ledger
        .register_worker("g/b", item_cost(4), &staying, None)
        .expect("admitted");
    push_memory_with_total(&departing, 20_000, 0, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].external_mb, 7_000, "32 − 20 − (4 + 1)");

    // A reading the surviving replica captured while the other was still resident,
    // but which is not ingested until after it left: settles are per replica, so
    // this ordering is ordinary.
    push_memory_with_total(&staying, 20_100, 0, Some(32_000), "nvml");

    drop(leaving);
    assert_eq!(
        ledger.health()[0].external_mb,
        7_000,
        "unchanged by the exit"
    );

    ledger.ingest_all_for_test();
    assert_eq!(
        ledger.health()[0].external_mb,
        7_000,
        "and a reading from before the exit does not undo the credit"
    );
    assert!(
        refresh_due(ledger.lock().gpus.get(GPU).expect("the GPU")),
        "the GPU is still waiting on a reading of its own"
    );

    // The driver settles it: only 21 GB came free, so a gigabyte of what
    // the credit assumed was ours is in fact somebody else's now.
    push_memory_with_total(&staying, 21_000, 0, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.external_mb, 10_000, "32 − 21 − 1, the reading's own");
    let state = ledger.lock();
    assert!(
        !refresh_due(state.gpus.get(GPU).expect("the GPU")),
        "a real reading clears the forced refresh with it"
    );
}

/// The credit is the *footprint*, not the base, and it survives being applied twice
/// in a row.
#[test]
fn back_to_back_departures_credit_each_replicas_grown_footprint() {
    let ledger = ledger(32_000, no_margin());
    // 4 GB of weights over a 1 GB load-time pool, and a second, quiet
    // replica whose pool never moved.
    let grown = loaded(Some(4_000), Some(1_000));
    let quiet = loaded(Some(1_000), Some(0));
    let first = ledger
        .register_worker("g/a", item_cost(4), &grown, None)
        .expect("admitted");
    let second = ledger
        .register_worker("g/b", item_cost(4), &quiet, None)
        .expect("admitted");
    // The pool grew to 3 GB, so `g/a`'s footprint is 4 000 + (3 000 −
    // 1 000) = 6 000 — half as much again as its base.
    push_memory_with_total(&grown, 20_000, 3_000, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.footprints_mb, 7_000, "6 000 grown + 1 000 quiet");
    assert_eq!(gpu.external_mb, 5_000, "32 − 20 − 7");

    drop(first);
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.footprints_mb, 1_000, "only the quiet replica is left");
    assert_eq!(
        gpu.external_mb, 5_000,
        "the whole footprint — pool growth included — was credited, not \
         just the base"
    );

    drop(second);
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.footprints_mb, 0, "the GPU is empty");
    assert_eq!(
        gpu.external_mb, 5_000,
        "the second departure credits against the first's adjusted figure"
    );
    assert!(
        refresh_due(ledger.lock().gpus.get(GPU).expect("the GPU")),
        "and the GPU is still waiting on a reading of its own"
    );
}

/// A departure from a GPU that has never had a free reading adjusts
/// nothing and flags nothing — and, in particular, does not leave a stamp
/// that would refuse the GPU's *first* reading when it finally lands.
#[test]
fn a_departure_from_a_gpu_with_no_reading_does_not_refuse_the_first_one() {
    let ledger = ledger(32_000, no_margin());
    let departing = loaded(Some(4_000), Some(0));
    let staying = loaded(Some(1_000), Some(0));
    let leaving = ledger
        .register_worker("g/a", item_cost(4), &departing, None)
        .expect("admitted");
    let _resident = ledger
        .register_worker("g/b", item_cost(4), &staying, None)
        .expect("admitted");
    assert!(
        !ledger.health()[0].external_known,
        "no reading has ever landed on this GPU"
    );

    drop(leaving);
    push_memory_with_total(&staying, 27_000, 0, Some(32_000), "nvml");
    ledger.ingest_all_for_test();
    let gpu = &ledger.health()[0];
    assert!(gpu.external_known, "the first reading was accepted");
    assert_eq!(gpu.external_mb, 4_000, "32 − 27 − 1, the reading's own");
}

/// The credit is gated on the reading having *counted* the departing footprint.
#[test]
fn a_reading_that_predates_the_load_is_not_credited() {
    let ledger = ledger(32_000, no_margin());
    // The GPU's only reading rides the first replica's load report, so
    // it is stamped before the second replica exists.
    let first = loaded(Some(1_000), Some(0));
    {
        let mut telemetry = first.lock().unwrap();
        let load = telemetry.load.as_mut().expect("the load report");
        load.value.memory = Some(MemorySample {
            free_mb: Some(20_000),
            total_mb: Some(32_000),
            free_source: Some("nvml".to_owned()),
            reserved_mb: Some(0),
            allocated_mb: Some(0),
            ..MemorySample::default()
        });
    }
    let _resident = ledger
        .register_worker("g/a", item_cost(4), &first, None)
        .expect("admitted");
    let late = loaded(Some(4_000), Some(0));
    let leaving = ledger
        .register_worker("g/b", item_cost(4), &late, None)
        .expect("admitted");
    assert_eq!(ledger.health()[0].external_mb, 7_000, "32 − 20 − (1 + 4)");

    drop(leaving);

    let gpu = &ledger.health()[0];
    assert_eq!(
        gpu.external_mb, 11_000,
        "the reading never saw the 4 GB, so there is none of it to give \
         back: external reads high rather than inventing headroom"
    );
    assert!(
        refresh_due(ledger.lock().gpus.get(GPU).expect("the GPU")),
        "and the probe is what settles it"
    );
}

/// The staleness refresh backs off after a failure.
#[test]
fn a_failed_external_refresh_backs_off() {
    let fresh = |free: Option<FreeSample>, failed: Option<Instant>, refreshing: bool| GpuLedger {
        name: "TEST 9000".to_owned(),
        total_mb: 10_000,
        free,
        refreshing,
        last_refresh_failed_at: failed,
        ..GpuLedger::default()
    };
    let stale = || {
        Some(FreeSample {
            free_mb: 1000,
            source: "nvml".to_owned(),
            at: Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1),
            ram: None,
        })
    };
    assert!(
        refresh_due(&fresh(None, None, false)),
        "no reading at all is worth a probe"
    );
    assert!(refresh_due(&fresh(stale(), None, false)), "stale reading");
    assert!(
        !refresh_due(&fresh(
            Some(FreeSample {
                free_mb: 1000,
                source: "nvml".to_owned(),
                at: Instant::now(),
                ram: None,
            }),
            None,
            false
        )),
        "a fresh reading needs nothing"
    );
    assert!(
        !refresh_due(&fresh(stale(), Some(Instant::now()), false)),
        "a probe that just failed is not retried immediately"
    );
    assert!(
        refresh_due(&fresh(
            stale(),
            Some(Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1)),
            false
        )),
        "an old failure no longer suppresses"
    );
    assert!(
        !refresh_due(&fresh(stale(), None, true)),
        "a probe already in flight for this GPU"
    );
    // The departure stamp forces a probe past the staleness clock, but it is the
    // weakest of the three conditions: a host whose `nvidia-smi` answers nothing
    // still buys its quiet period, and a probe already in flight still answers for
    // it.
    let adjusted = |failed: Option<Instant>, refreshing: bool| {
        let mut gpu = fresh(
            Some(FreeSample {
                free_mb: 1000,
                source: "nvml".to_owned(),
                at: Instant::now(),
                ram: None,
            }),
            failed,
            refreshing,
        );
        gpu.free_adjusted_at = Some(Instant::now());
        gpu
    };
    assert!(
        refresh_due(&adjusted(None, false)),
        "an adjusted reading is probed however fresh its own timestamp"
    );
    assert!(
        !refresh_due(&adjusted(Some(Instant::now()), false)),
        "but a probe that just failed still wins over the stamp"
    );
    assert!(
        !refresh_due(&adjusted(None, true)),
        "and so does one already in flight"
    );
}

/// A GPU with no resident has never been probed — `request_grant` is the only
/// other trigger and it needs a worker to hang off — so the load path probes it
/// itself.
#[tokio::test]
async fn a_load_reservation_probes_a_gpu_with_no_reading() {
    let ledger = ledger(97_887, no_margin());
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 97_887,
        free_mb: 2_271,
    }]));
    assert!(
        !ledger.health()[0].external_known,
        "nothing has ever read this GPU"
    );

    let (reservation, exceeds_headroom) = ledger
        .reserve_load_signalling_for_test("g/nemotron", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    assert_eq!(ledger.probe_calls(), 1, "the load path probed the host");
    {
        // A probe that *answered* leaves neither the in-flight flag nor a
        // failure backoff behind: `record_external_probe` settles both and
        // `ProbeGuard` is disarmed, so the next stale reading is re-probed
        // immediately rather than sitting out a backoff it never earned.
        let state = ledger.lock();
        let gpu = state.gpus.get(GPU).expect("the GPU");
        assert!(!gpu.refreshing, "the in-flight flag was settled");
        assert!(
            gpu.last_refresh_failed_at.is_none(),
            "and a probe that answered bought no failure backoff"
        );
    }
    let gpu = &ledger.health()[0];
    assert!(gpu.external_known, "and priced the load against a reading");
    assert_eq!(
        gpu.external_mb, 95_616,
        "97_887 − 2_271, with no resident of ours to net off"
    );
    assert_eq!(gpu.limit_mb, 2_271, "at margin 0 the limit is what is free");
    assert_eq!(
        gpu.load_reservations_mb, 2_271,
        "the placeholder is clamped to the headroom it is priced against"
    );
    assert!(
        exceeds_headroom,
        "4 GiB expected against 2 271 MiB of headroom: the \
         evict-before-load signal fires"
    );

    drop(reservation);
    assert_eq!(ledger.health()[0].load_reservations_mb, 0);
}

/// The placeholder base is a guess, so it may not be reserved past the
/// headroom: on a squeezed board the ledger invariant `charges + load
/// reservations <= limit_mb` holds, and the evict-before-load signal still
/// fires on the *expected* figure that did not fit.
#[tokio::test]
async fn a_placeholder_reservation_is_clamped_to_the_headroom() {
    let ledger = ledger(32_606, no_margin());
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 32_606,
        free_mb: 196,
    }]));
    let (reservation, exceeds_headroom) = ledger
        .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.limit_mb, 196, "at margin 0 the limit is what is free");
    assert_eq!(
        gpu.load_reservations_mb, 196,
        "196 MiB of headroom reserves 196, not the 4 GiB placeholder"
    );
    assert!(
        gpu.charges_mb + gpu.load_reservations_mb <= gpu.limit_mb,
        "the ledger invariant holds: {} + {} vs {}",
        gpu.charges_mb,
        gpu.load_reservations_mb,
        gpu.limit_mb
    );
    assert!(
        exceeds_headroom,
        "and the clamp does not silence the evict-before-load signal"
    );
    drop(reservation);
    assert_eq!(ledger.health()[0].load_reservations_mb, 0);
}

/// The load probe is the staleness refresh's rule applied on a second
/// path, not a second policy: a GPU whose reading is current is not
/// re-read, so a busy host pays nothing for this.
#[tokio::test]
async fn a_fresh_reading_suppresses_the_load_probe() {
    let ledger = ledger(32_000, no_margin());
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: 32_000,
        free_mb: 1_000,
    }]));
    ledger.lock().gpus.get_mut(GPU).expect("the GPU").free = Some(FreeSample {
        free_mb: 20_000,
        source: "nvml".to_owned(),
        at: Instant::now(),
        ram: None,
    });

    let (_reservation, exceeds_headroom) = ledger
        .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    assert_eq!(ledger.probe_calls(), 0, "a reading this fresh needs none");
    assert_eq!(
        ledger.health()[0].external_mb,
        12_000,
        "the sample the GPU already had, not the stub's 31 000"
    );
    assert!(!exceeds_headroom, "4 GiB against 20 000 MiB of headroom");
}

/// And the failure backoff wins on this path too: a host whose probe
/// answers nothing must not pay a timed-out subprocess per load attempt —
/// a model that fails to load is retried.
#[tokio::test]
async fn a_failed_probe_suppresses_the_next_load_probe() {
    let ledger = ledger(32_000, no_margin());
    ledger.install_probe_stub(None);

    let first = ledger
        .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    assert_eq!(ledger.probe_calls(), 1);
    assert!(
        !ledger.health()[0].external_known,
        "the probe answered nothing, so the GPU is still unread"
    );
    drop(first);

    let _second = ledger
        .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    assert_eq!(
        ledger.probe_calls(),
        1,
        "still inside the backoff window the first failure bought"
    );
}

/// A probe that enumerates *some other* GPU is a failure for the GPU
/// the load is being priced against, and must be accounted as one — the
/// GPU it did answer for still gets the reading (the snapshot is real),
/// but the pinned GPU stays unread, keeps its full-total headroom, and
/// buys the same backoff a probe that answered nothing would.
#[tokio::test]
async fn a_probe_that_misses_the_pinned_gpu_backs_off_like_a_failure() {
    const OTHER: &str = "GPU-bbbb";
    let ledger = VramLedger::for_test(
        &[(GPU, "TEST 9000", 32_000), (OTHER, "TEST 9000", 32_000)],
        no_margin(),
    );
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: OTHER.to_owned(),
        total_mb: 32_000,
        free_mb: 1_000,
    }]));

    let _first = ledger
        .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    assert_eq!(ledger.probe_calls(), 1);
    let gpus = ledger.health();
    let pinned = gpus.iter().find(|b| b.gpu_uuid == GPU).unwrap();
    let other = gpus.iter().find(|b| b.gpu_uuid == OTHER).unwrap();
    assert!(
        !pinned.external_known,
        "the snapshot said nothing about this GPU"
    );
    assert_eq!(pinned.limit_mb, 32_000, "so it is still priced as empty");
    assert!(
        other.external_known,
        "the GPU the snapshot did cover is not thrown away with it"
    );
    assert_eq!(other.external_mb, 31_000);

    let _second = ledger
        .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    assert_eq!(
        ledger.probe_calls(),
        1,
        "a GPU this probe never enumerates must not pay a subprocess per \
         load attempt"
    );
}

/// One probe answers for every GPU it enumerates, so a load pinned to
/// several GPUs pays exactly one: the first GPU's probe records the
/// rest, and `refresh_due` is false for them by the time they are priced.
#[tokio::test]
async fn one_probe_serves_every_gpu_a_load_is_pinned_to() {
    const OTHER: &str = "GPU-bbbb";
    let ledger = VramLedger::for_test(
        &[(GPU, "TEST 9000", 32_000), (OTHER, "TEST 9000", 24_000)],
        no_margin(),
    );
    ledger.install_probe_stub(Some(vec![
        GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 32_000,
            free_mb: 2_000,
        },
        GpuMemory {
            uuid: OTHER.to_owned(),
            total_mb: 24_000,
            free_mb: 3_000,
        },
    ]));

    let _one = ledger
        .reserve_load_for_test("g/a", item_cost(4), GPU, None)
        .await;
    let _two = ledger
        .reserve_load_for_test("g/a", item_cost(4), OTHER, None)
        .await;
    assert_eq!(
        ledger.probe_calls(),
        1,
        "the second GPU was already measured by the first GPU's probe"
    );
    let gpus = ledger.health();
    let pinned = gpus.iter().find(|b| b.gpu_uuid == GPU).unwrap();
    let other = gpus.iter().find(|b| b.gpu_uuid == OTHER).unwrap();
    assert_eq!(pinned.external_mb, 30_000);
    assert_eq!(other.external_mb, 21_000);
}

/// A probe that *unwinds* must leave the GPU refreshable.
#[test]
fn a_panicking_probe_leaves_the_gpu_refreshable() {
    let ledger = ledger(32_000, no_margin());
    ledger.install_panicking_probe_stub();
    // The panic travels: probe stub → blocking pool → `JoinError` →
    // `resume_unwind` in the load path → here.
    let reserve = |ledger: &Arc<VramLedger>| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime for one reservation");
        drop(runtime.block_on(ledger.reserve_load_for_test("g/a", item_cost(4), GPU, None)));
    };
    // The panics below are the point of the test; the default hook would
    // print a backtrace for each.
    let quietly = |body: &dyn Fn()| {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
        std::panic::set_hook(hook);
        outcome
    };

    let outcome = quietly(&|| reserve(&ledger));
    assert!(outcome.is_err(), "the probe panicked through the load path");
    assert_eq!(ledger.probe_calls(), 1);
    {
        let state = ledger.lock();
        let gpu = state.gpus.get(GPU).expect("the GPU");
        assert!(
            !gpu.refreshing,
            "the guard cleared the in-flight flag on the unwind"
        );
        assert!(
            gpu.last_refresh_failed_at.is_some(),
            "and stamped the failure backoff, so the next request does not \
             walk straight back into a query that is panicking on this host"
        );
        assert!(!refresh_due(gpu), "which is why it is not due right now");
    }

    // Once that backoff expires the GPU is due again — which it never
    // would be if the flag were still latched.
    ledger
        .lock()
        .gpus
        .get_mut(GPU)
        .expect("the GPU")
        .last_refresh_failed_at =
        Some(Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1));
    assert!(
        refresh_due(ledger.lock().gpus.get(GPU).expect("the GPU")),
        "the panic cost this GPU one backoff window, not every future \
         refresh"
    );

    // End to end: the next load reservation really does probe again.
    let outcome = quietly(&|| reserve(&ledger));
    assert!(outcome.is_err());
    assert_eq!(
        ledger.probe_calls(),
        2,
        "refreshes for this GPU were not silently disabled"
    );
}

/// Reading telemetry by watermark is what makes ring overflow visible: the
/// fit knows it has a hole rather than assuming continuity.
#[test]
fn a_telemetry_ring_overflow_is_detectable() {
    assert_eq!(watermark_gap(Some(1), 0), 0, "continuous from the start");
    assert_eq!(watermark_gap(Some(5), 4), 0, "continuous");
    assert_eq!(watermark_gap(Some(6), 4), 1, "seq 5 was evicted");
    assert_eq!(watermark_gap(None, 0), 0, "nothing recorded yet");
    assert_eq!(watermark_gap(Some(3), 9), 0, "already read past it");

    // End to end: more measurements than the ring holds, in one window.
    let ledger = ledger(1_000_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 900_000, 0);
    let flood: Vec<BatchMeasurement> = (1..=(WorkerTelemetry::RING as u64 + 10))
        .map(|k| measurement(k, 0, 10 * k + 100))
        .collect();
    let recorded = flood.len() as u64;
    handle.lock().unwrap().record_measurements(flood);
    clean_window(&admission);
    // The retained tail was ingested; the evicted head is simply missing.
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        recorded,
        "the newest samples still land"
    );
    assert!(
        fit_sample_count(&ledger) <= WorkerTelemetry::RING,
        "and no more than the ring held"
    );
}

/// An aborted window teaches the ledger nothing about the ramp — but its
/// measurements must not be left in the ring for the *next* window to be
/// blamed (or credited) for.
#[test]
fn an_aborted_windows_telemetry_is_not_charged_to_the_next_one() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    // A window runs one OOM batch and is then aborted (its worker died, the
    // dispatcher tore down, the task was dropped).
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            ..measurement(4, 0, 900)
        }]);
    token.finish(WindowOutcome::Aborted);
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.deflation, 0, "an aborted window does not deflate");
    assert_eq!(worker.ramp_step, 0, "and earns no growth");

    // The next window is clean and measured.
    assert_eq!(measured_window(&handle, &admission, 4), 4);
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.deflation, 0,
        "the aborted window's OOM was watermarked away, not inherited"
    );
    assert_eq!(
        worker.ramp_step, 1,
        "the clean measured window earned a step"
    );
}

/// Off CUDA the retry counter and the release count are **absent**, not
/// zero: an MPS or CPU replica keeps no `num_alloc_retries` and releases
/// nothing, and reading 0 there is indistinguishable from a CUDA card that
/// was never short of memory.
#[test]
fn health_reads_absence_not_zero_for_a_worker_off_cuda() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let resident = ledger
        .register_worker("g/mps", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 6000, 1000);
    ledger.ingest_all_for_test();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(4, 0, 900)]);
    clean_window(&resident);
    // A trim it answered with no figure at all, which is what a worker
    // with no live CUDA replies.
    resident.note_trimmed(TrimReply::default());
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.alloc_retries_last_window, None);
    assert_eq!(worker.alloc_retries_total, None, "no counter to total");
    assert_eq!(worker.pool_releases, None, "nothing was measured");
    assert_eq!(worker.last_release_mb, None);

    // A CUDA replica that measured a zero of each says so.
    let cuda = loaded(Some(1000), Some(0));
    let on_cuda = ledger
        .register_worker("g/cuda", item_cost(4), &cuda, None)
        .unwrap();
    push_memory(&cuda, 6000, 1000);
    ledger.ingest_all_for_test();
    cuda.lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            alloc_retries: Some(0),
            ..measurement(4, 0, 900)
        }]);
    clean_window(&on_cuda);
    on_cuda.note_trimmed(released(0));
    let health = ledger.health();
    let worker = health[0]
        .workers
        .iter()
        .find(|worker| worker.inference_id == "g/cuda")
        .expect("registered");
    assert_eq!(worker.alloc_retries_last_window, Some(0));
    assert_eq!(worker.alloc_retries_total, Some(0));
    assert_eq!(worker.pool_releases, Some(0));
}
/// The ledger half of the same additivity claim: a frame with no
/// `reserved_after_mb` — a worker too old to send one, on any backend —
/// charges the pool from the peak, so it is priced exactly as it was
/// before the field existed. (The wire half lives beside the parser,
/// `worker::tests::a_frame_too_old_for_the_round_6_fields_parses_as_it_did_before`.)
#[test]
fn a_frame_with_no_post_batch_pool_is_priced_from_the_peak_as_before() {
    let ledger = ledger(24_576, no_margin());
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .expect("registers");
    let token = admission.request_grant(8, None, 1, 0).expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement_with_free(8, 1_000, 1_400, 18_000, "nvml")]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].external_mb,
        24_576 - 18_000 - (1_000 + 1_400),
        "the peak is still the pool a frame without `reserved_after` \
         charges, measured from `reserved_at_load` = 0"
    );
}

/// The CUDA branch is the arithmetic `24820452` shipped:
/// `total - free - (base + pool growth)`, which is what nvidia-smi's
/// reserved figure counts. Round 6 renamed the helper; it did not change
/// this branch.
#[test]
fn the_cuda_branch_still_nets_what_nvidia_smi_counts() {
    let ledger = ledger(24_576, no_margin());
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    for (free, pool) in [(20_000u64, 0u64), (18_000, 2_000), (18_000, 5_000)] {
        push_memory(&handle, free, pool);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].external_mb,
            24_576 - free - (1_000 + pool),
            "free {free}, pool {pool}"
        );
    }
}

/// The probe path a CUDA host takes is byte-identical to round 5's: the
/// RAM branch is gated on the Metal allocator flag. A probe before the
/// first worker prices `total - free - reserve` as it always did, with no
/// RAM basis attached.
#[tokio::test]
async fn a_cuda_probe_before_the_first_worker_prices_as_before() {
    const TOTAL: u64 = 24_576;
    const HOG: u64 = 20_000;
    let ledger = ledger(TOTAL, no_margin());
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: GPU.to_owned(),
        total_mb: TOTAL,
        free_mb: TOTAL - HOG,
    }]));
    let (_reservation, exceeds) = ledger
        .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
        .await
        .expect("a known GPU charges the load");
    let gpu = &ledger.health()[0];
    assert_eq!(gpu.total_mb, TOTAL);
    assert_eq!(gpu.external_mb, HOG);
    assert_eq!(gpu.limit_mb, TOTAL - HOG - gpu.reserve_mb);
    assert!(!exceeds);
}

/// A frame with `reserved_after_mb` absent falls back to the peak for
/// **both** readings it feeds — the resident's charge and `grew_pool` — so
/// an old worker keeps round 5's behaviour rather than losing the reading
/// altogether.
#[test]
fn a_frame_without_a_post_batch_pool_falls_back_to_the_peak() {
    let (ledger, handle, admission) = ramping_from_seed(1);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let units = token.grant().unit_budget;
    // peak above `before`: pool-growing, so not warm, so no ring sample.
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(units, 0, 10 * units + 100)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].workers[0].throughput_samples,
        0,
        "a peak above the pre-batch pool is still `grew_pool = true`"
    );
}

/// Dropping the RAM basis from a CUDA batch frame is inert, because a CUDA
/// frame never carries one. The RAM branch is double-gated on
/// `metal_allocator` and on the frame's own pair.
#[test]
fn a_ram_basis_on_a_cuda_frame_changes_nothing() {
    let priced = |basis: bool| {
        let ledger = ledger(24_576, no_margin());
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .expect("registers");
        let token = admission.request_grant(8, None, 1, 0).expect("granted");
        let mut batch = measurement_with_free(8, 0, 400, 18_000, "nvml");
        if basis {
            batch.ram_total_mb = Some(64 * 1024);
            batch.ram_available_mb = Some(30_000);
        }
        handle.lock().unwrap().record_measurements(vec![batch]);
        token.finish(WindowOutcome::Responded { oom: None });
        ledger.health()[0].external_mb
    };
    assert_eq!(priced(true), priced(false));
}
