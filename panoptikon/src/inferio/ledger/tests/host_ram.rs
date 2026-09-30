//! A GPU replica's host RAM, booked on the CPU device beside its GPU memory.
use super::*;
use crate::inferio::gpu::GpuInventory;
use crate::inferio::ledger::health::LedgerWorkerHealth;

/// The resident set of every GPU replica here at load, and the host RAM
/// each unit of its batches costs above it.
const RSS_AT_LOAD_MB: u64 = 2_000;
const RAM_PER_UNIT_MB: u64 = 10;

/// Throughput that keeps rising with batch size, so the ramp never holds.
const RISING: [(u64, f64); 2] = [(1, 100.0), (1 << 20, 2_100.0)];

/// CUDA cards beside the CPU device, as `VramLedger::new` builds a GPU host.
/// With no margin, a CPU free reading of `n` MiB leaves `n` MiB to book while
/// every resident set is at its load baseline. The host is not probed: free
/// readings are the test's own, or a probe stub's.
fn host(cards: &[&str], profiles: Option<Arc<FakeProfiles>>) -> Arc<VramLedger> {
    host_with_ram(cards, profiles, CPU_RAM_MB)
}

fn host_with_ram(
    cards: &[&str],
    profiles: Option<Arc<FakeProfiles>>,
    ram_mb: u64,
) -> Arc<VramLedger> {
    let inventory = GpuInventory::known(
        cards
            .iter()
            .enumerate()
            .map(|(index, uuid)| nvidia(index as u32, uuid, "TEST 9000", 200_000))
            .collect(),
    )
    .with_cpu(ram_mb, crate::inferio::cpu::MemRoots::default());
    let mut ledger = VramLedger::new(
        &inventory,
        no_margin().into(),
        profiles.map(|profiles| profiles as Arc<dyn CalibrationProfiles>),
    );
    Arc::get_mut(&mut ledger)
        .expect("not shared yet")
        .probe_external = false;
    ledger
}

/// `handle`'s load report with the worker's resident set at load.
fn with_rss(handle: TelemetryHandle) -> TelemetryHandle {
    handle
        .lock()
        .unwrap()
        .load
        .as_mut()
        .expect("a load report")
        .value
        .rss_at_load_mb = Some(RSS_AT_LOAD_MB);
    handle
}

fn gpu_replica(
    ledger: &Arc<VramLedger>,
    model: &str,
    gpu: &str,
    seed: u32,
) -> (TelemetryHandle, Admission) {
    let handle = with_rss(loaded_on(gpu, Some(1000), Some(0)));
    let admission = ledger
        .register_worker(model, item_cost(seed), &handle, Some(gpu))
        .expect("admitted on its card");
    push_memory(&handle, 190_000, 1000);
    (handle, admission)
}

/// [`ramp_window`] at [`RISING`] from a worker that reports its host RAM:
/// every batch peaks [`RAM_PER_UNIT_MB`] per unit over [`RSS_AT_LOAD_MB`]
/// and hands it back after. Returns the grant.
fn ram_window(handle: &TelemetryHandle, admission: &Admission) -> Grant {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let grant = *token.grant();
    let units = grant.unit_budget;
    let rate = ladder_rate(&RISING, units);
    let host_ram = |batch: BatchMeasurement| BatchMeasurement {
        peak_rss_mb: Some(RSS_AT_LOAD_MB + RAM_PER_UNIT_MB * units),
        rss_after_mb: Some(RSS_AT_LOAD_MB),
        ..batch
    };
    let mut batches = vec![host_ram(BatchMeasurement {
        duration_ms: Some(units as f64 * 1000.0 / rate),
        ..measurement(units, 0, 10 * units + 100)
    })];
    batches.extend((1..WINDOW_DEPTH_MULTIPLIER).map(|_| host_ram(warm_batch(units, rate))));
    handle.lock().unwrap().record_measurements(batches);
    token.finish(WindowOutcome::Responded { oom: None });
    grant
}

/// A batch at `units` with its resident set's in-batch peak and level after.
fn ram_batch(units: u64, peak_mb: u64, after_mb: u64) -> BatchMeasurement {
    BatchMeasurement {
        peak_rss_mb: Some(peak_mb),
        rss_after_mb: Some(after_mb),
        ..measurement(units, 0, 10 * units + 100)
    }
}

/// One window whose single batch keeps all the host RAM it used; no new
/// CPU free reading is taken. Returns its unit budget.
fn kept_window(handle: &TelemetryHandle, admission: &Admission) -> u64 {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let units = token.grant().unit_budget;
    let kept = RSS_AT_LOAD_MB + RAM_PER_UNIT_MB * units;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            peak_rss_mb: Some(kept),
            rss_after_mb: Some(kept),
            ..measurement(units, 0, 10 * units + 100)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    units
}

fn row(ledger: &Arc<VramLedger>, model: &str) -> LedgerWorkerHealth {
    ledger
        .health()
        .into_iter()
        .flat_map(|gpu| gpu.workers)
        .find(|worker| worker.inference_id == model)
        .expect("a resident replica")
}

fn cpu_row(ledger: &Arc<VramLedger>) -> GpuBudgetHealth {
    ledger
        .health()
        .into_iter()
        .find(|gpu| gpu.gpu_uuid == cpu::DEVICE_KEY)
        .expect("the CPU device")
}

/// The ramp, knee and anchor state a RAM ceiling must leave alone.
fn ramp_state(row: &LedgerWorkerHealth) -> (u32, u32, usize, Option<u64>, bool) {
    (
        row.ramp_step,
        row.deflation,
        row.throughput_samples,
        row.knee_units,
        row.ramp_held,
    )
}

/// With RAM to spare, a replica that books it is granted exactly what one on
/// a host without a CPU device is, window after window.
#[test]
fn plentiful_host_ram_changes_no_grant() {
    let base = ledger(200_000, no_margin());
    let base_handle = loaded(Some(1000), Some(0));
    let base_admission = base
        .register_worker("g/plenty", item_cost(8), &base_handle, None)
        .expect("admitted");
    push_memory(&base_handle, 190_000, 1000);
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/plenty", GPU, 8);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);

    for window in 0..10 {
        let expected = ram_window(&base_handle, &base_admission);
        assert_eq!(ram_window(&handle, &admission), expected, "window {window}");
        let (booked, unbooked) = (row(&ledger, "g/plenty"), row(&base, "g/plenty"));
        assert_eq!(
            ramp_state(&booked),
            ramp_state(&unbooked),
            "window {window}"
        );
        assert_eq!(booked.max_units_measured, unbooked.max_units_measured);
    }
    let booked = row(&ledger, "g/plenty");
    assert_eq!(booked.unit_budget, 8 << 10, "still ramping");
    assert_eq!(booked.ram_resident_mb, Some(RSS_AT_LOAD_MB));
    assert_eq!(booked.ram_mb_per_unit, Some(RAM_PER_UNIT_MB as f64));
    assert_eq!(booked.ram_booked_mb, 0, "no grant outstanding");
    assert!(!booked.ram_ceiling_binding);
}

/// When host RAM gets tight mid-job the next grant is what it holds, like
/// the edge of a full card: the ramp keeps its place, the knee ring takes
/// nothing and nothing deflates. When RAM frees up the ramp resumes where it
/// stood.
#[test]
fn host_ram_caps_a_gpu_replica_without_moving_its_ramp() {
    let ledger = host(&[GPU], None);
    // A seed of 3 puts the capped size in a larger log2 bucket than the
    // last size the ramp reached.
    let (handle, admission) = gpu_replica(&ledger, "g/capped", GPU, 3);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let ramped: Vec<u64> = (0..7)
        .map(|_| ram_window(&handle, &admission).unit_budget)
        .collect();
    assert_eq!(ramped, [3, 6, 12, 24, 48, 96, 192]);
    let before = row(&ledger, "g/capped");
    assert_eq!(before.unit_budget, 384, "the ramp's next size");

    // 3 000 MiB left to book: 300 units at 10 MiB each.
    ledger.record_free_for_test(cpu::DEVICE_KEY, 3_000);
    let logs = captured_logs(|| {
        for _ in 0..3 {
            let grant = ram_window(&handle, &admission);
            assert_eq!(grant.unit_budget, 300);
            assert_eq!(grant.mb, 3_000, "the GPU reserves only what 300 units need");
            assert!(grant.squeezed, "memory held it back");
        }
    });
    let capped_lines = logs
        .iter()
        .filter(|(level, message)| {
            *level == tracing::Level::INFO && message.contains("host RAM capped")
        })
        .count();
    assert_eq!(capped_lines, 1, "throttled per model and GPU");
    let after = row(&ledger, "g/capped");
    assert!(after.ram_ceiling_binding);
    assert_eq!(ramp_state(&after), ramp_state(&before));
    assert_eq!(after.max_units_measured, 300, "a clean batch it did run");

    // Tighter still: the next grant shrinks below the last.
    ledger.record_free_for_test(cpu::DEVICE_KEY, 1_000);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 100);

    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 384);
    assert!(!row(&ledger, "g/capped").ram_ceiling_binding);
}

/// A window host RAM cut below the knee did not run at the knee, so it is no
/// evidence for widening it.
#[test]
fn a_ram_capped_window_does_not_expire_the_knee() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/knee", GPU, 64);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    ram_window(&handle, &admission);
    ledger.set_knee_for_test("g/knee", GPU, 100);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 500);
    for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
        assert_eq!(ram_window(&handle, &admission).unit_budget, 50);
    }
    assert_eq!(row(&ledger, "g/knee").knee_units, Some(100));
}

/// Two GPU replicas growing at once cannot both claim the same host RAM: a
/// grant's booking holds until it settles, whatever the worker reports
/// between its batches.
#[test]
fn two_gpu_replicas_cannot_book_the_same_host_ram() {
    const OTHER: &str = "GPU-bbbb";
    let ledger = host(&[GPU, OTHER], None);
    let (a_handle, a) = gpu_replica(&ledger, "g/book-a", GPU, 256);
    let (b_handle, b) = gpu_replica(&ledger, "g/book-b", OTHER, 256);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    ram_window(&a_handle, &a);
    ram_window(&b_handle, &b);

    // 3 000 MiB left to book; each replica's ramp wants 512 units (5 120 MiB).
    ledger.record_free_for_test(cpu::DEVICE_KEY, 3_000);
    let first = a.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(first.grant().unit_budget, 300);
    assert_eq!(row(&ledger, "g/book-a").ram_booked_mb, 3_000);
    // A batch of the first window finished and freed its RAM.
    a_handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            peak_rss_mb: Some(RSS_AT_LOAD_MB + 3_000),
            rss_after_mb: Some(RSS_AT_LOAD_MB),
            ..measurement(300, 0, 3_100)
        }]);
    let second = b.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(second.grant().unit_budget, 1, "only the one-unit floor");
    drop(second);

    first.finish(WindowOutcome::Responded { oom: None });
    let second = b.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(second.grant().unit_budget, 300, "released at settle");
}

/// A GPU replica's resident set is ours on the CPU device, counted once:
/// not external usage too, whether the free reading predates its load or the
/// memory it kept after a window. Gone, it is credited back to the reading.
#[test]
fn a_gpu_replicas_resident_set_is_ours_on_the_cpu_device() {
    const OTHERS: u64 = 50_000;
    let ledger = host(&[GPU], None);
    ledger.record_free_for_test(cpu::DEVICE_KEY, CPU_RAM_MB - OTHERS);
    let (handle, admission) = gpu_replica(&ledger, "g/resident", GPU, 256);
    let cpu = cpu_row(&ledger);
    assert_eq!(cpu.external_mb, OTHERS);
    assert_eq!(cpu.footprints_mb, RSS_AT_LOAD_MB);
    assert_eq!(cpu.charges_mb, RSS_AT_LOAD_MB);
    assert_eq!(cpu.headroom_mb, CPU_RAM_MB - OTHERS - RSS_AT_LOAD_MB);

    assert_eq!(kept_window(&handle, &admission), 256);
    let cpu = cpu_row(&ledger);
    assert_eq!(cpu.external_mb, OTHERS);
    assert_eq!(cpu.footprints_mb, RSS_AT_LOAD_MB + 2_560);

    drop(admission);
    assert_eq!(cpu_row(&ledger).external_mb, OTHERS);
}

/// RAM a replica kept after its window is gone from the CPU free reading
/// before the next reading is taken: it is booked once, not again.
#[test]
fn ram_kept_after_a_window_is_not_booked_again() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/kept-once", GPU, 256);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 8_000);
    ram_window(&handle, &admission);
    assert_eq!(kept_window(&handle, &admission), 512);
    assert!(
        ledger.lock().gpus[cpu::DEVICE_KEY]
            .free_adjusted_at
            .is_some(),
        "a reading older than the change is refused, and a new one is due"
    );
    // 2 880 MiB free and the 5 120 it kept: 800 units, not the ramp's 1 024.
    let next = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(next.grant().unit_budget, 800);
}

/// A reading taken after the window's batches already counts what they
/// kept, so it is left as it is.
#[test]
fn a_reading_newer_than_the_window_is_not_moved() {
    const OTHERS: u64 = 50_000;
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/newer", GPU, 256);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let kept = RSS_AT_LOAD_MB + 2_560;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![ram_batch(256, kept, kept)]);
    ledger.record_free_for_test(cpu::DEVICE_KEY, CPU_RAM_MB - OTHERS - kept);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(cpu_row(&ledger).external_mb, OTHERS);
}

/// Batches of one reply may carry one capture time (a coarse clock): the
/// window's change in resident set still moves the reading once, in full.
#[test]
fn batches_stamped_alike_still_move_the_reading() {
    const OTHERS: u64 = 50_000;
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/alike", GPU, 256);
    ledger.record_free_for_test(cpu::DEVICE_KEY, CPU_RAM_MB - OTHERS - RSS_AT_LOAD_MB);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let units = token.grant().unit_budget;
    handle.lock().unwrap().record_measurements_stamped_alike(
        [1_000, 3_000, 5_000]
            .map(|kept| ram_batch(units, RSS_AT_LOAD_MB + kept, RSS_AT_LOAD_MB + kept))
            .to_vec(),
    );
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(cpu_row(&ledger).external_mb, OTHERS);
    assert_eq!(
        row(&ledger, "g/alike").ram_resident_mb,
        Some(RSS_AT_LOAD_MB + 5_000)
    );
}

/// One-time host growth on the first batch (CUDA and library start-up) is
/// separated out as a fixed part once a second size ran, so a model cheap
/// per unit reaches the ceiling its RAM allows rather than one priced as if
/// every unit carried that growth.
#[test]
fn one_time_growth_does_not_hold_a_cheap_model_down() {
    const INIT: u64 = 1_500;
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/cheap", GPU, 64);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 3_000);
    let mut last = 0;
    for _ in 0..12 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        last = token.grant().unit_budget;
        let resident = RSS_AT_LOAD_MB + INIT;
        handle.lock().unwrap().record_measurements(vec![ram_batch(
            last,
            resident + last,
            resident,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    // 3 000 MiB free before start-up took 1 500: 1 500 units at 1 MiB each.
    assert_eq!(last, 1_500);
    assert!(row(&ledger, "g/cheap").ram_ceiling_binding);
    let _token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        row(&ledger, "g/cheap").ram_booked_mb,
        INIT + 1_500,
        "the fixed part is booked with the units"
    );
}

/// Two replicas together book no more new RAM than is truly free, while one
/// of them keeps what it grew.
#[test]
fn two_replicas_book_no_more_than_is_truly_free() {
    const OTHER: &str = "GPU-bbbb";
    let ledger = host(&[GPU, OTHER], None);
    let (a_handle, a) = gpu_replica(&ledger, "g/free-a", GPU, 256);
    let (b_handle, b) = gpu_replica(&ledger, "g/free-b", OTHER, 256);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    ram_window(&a_handle, &a);
    ram_window(&b_handle, &b);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 6_000);
    assert_eq!(kept_window(&a_handle, &a), 512);

    // 880 MiB truly free; A may also reuse the 5 120 it kept.
    let b_grant = b.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(b_grant.grant().unit_budget, 88);
    let a_grant = a.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(a_grant.grant().unit_budget, 512);
}

/// A probe stub answering `free_mb` for the CPU device.
fn host_ram_free(ledger: &VramLedger, free_mb: u64) {
    ledger.install_probe_stub(Some(vec![GpuMemory {
        uuid: cpu::DEVICE_KEY.to_owned(),
        total_mb: CPU_RAM_MB,
        free_mb,
    }]));
}

/// Each grant to a replica that books host RAM reads the host's free RAM
/// first, so RAM another process took since the last grant shrinks the very
/// next one.
#[test]
fn a_grant_reads_host_ram_first() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/fresh", GPU, 256);
    host_ram_free(&ledger, 45_000);
    ram_window(&handle, &admission);
    assert_eq!(
        cpu_row(&ledger).external_mb,
        CPU_RAM_MB - 45_000 - RSS_AT_LOAD_MB
    );
    assert_eq!(ram_window(&handle, &admission).unit_budget, 512);

    // Another process takes 42 000 MiB between two grants.
    host_ram_free(&ledger, 3_000);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 300);
}

/// Memory a GPU replica kept after its last batch is its own to reuse: it
/// counts as ours on the CPU device and as room for its own next grant.
#[test]
fn a_gpu_replica_reuses_the_ram_it_kept() {
    const KEPT: u64 = 500;
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/kept", GPU, 256);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    ram_window(&handle, &admission);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().unit_budget, 512);
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            peak_rss_mb: Some(RSS_AT_LOAD_MB + 5_120),
            rss_after_mb: Some(RSS_AT_LOAD_MB + KEPT),
            ..measurement(512, 0, 5_220)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        row(&ledger, "g/kept").ram_resident_mb,
        Some(RSS_AT_LOAD_MB + KEPT)
    );

    // 1 000 MiB free beside it: 1 500 MiB to book, 150 units.
    ledger.record_free_for_test(cpu::DEVICE_KEY, 1_000);
    assert_eq!(cpu_row(&ledger).footprints_mb, RSS_AT_LOAD_MB + KEPT);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 150);
}

/// A batch books the fixed part the fit separates once two sizes ran, plus
/// per unit the largest cost above it among batches within twice the
/// largest.
#[test]
fn the_ram_cost_is_the_fixed_part_plus_an_upper_per_unit_cost() {
    let ring = |samples: &[(u64, u64)]| -> Vec<FitSample> {
        samples
            .iter()
            .map(|&(units, delta_mb)| FitSample { units, delta_mb })
            .collect()
    };
    let cost = |fixed_mb: f64, mb_per_unit: f64| {
        Some(RamCost {
            fixed_mb,
            mb_per_unit,
        })
    };
    assert_eq!(ram_cost(&ring(&[])), None);
    assert_eq!(
        ram_cost(&ring(&[(8, 280)])),
        cost(0.0, 35.0),
        "one size: the fixed part is priced per unit"
    );
    assert_eq!(ram_cost(&ring(&[(16, 360), (8, 280)])), cost(200.0, 10.0));
    assert_eq!(
        ram_cost(&ring(&[(16, 360), (32, 520), (48, 1_400), (64, 840)])),
        cost(200.0, 25.0),
        "a batch of costly inputs raises the per-unit cost"
    );
    assert_eq!(
        ram_cost(&ring(&[(8, 1_000), (32, 520), (64, 840)])),
        cost(0.0, 16.25),
        "the 8-unit batch, reading memory kept from a larger one, is too small to count"
    );
    assert_eq!(ram_cost(&ring(&[(8, 0)])), None, "0 is unknown, not free");
    assert_eq!(
        ram_cost(&ring(&[(10, 50), (20, 150), (40, 350)])),
        cost(0.0, 10.0),
        "a negative fixed part is 0"
    );
}

/// Start-up growth and cost per unit of the retention tests' model, whose
/// worker keeps every MiB a batch peaked at (glibc's default on a GPU
/// worker).
const RETAINED_INIT_MB: u64 = 2_000;
const RETAINED_PER_UNIT_MB: u64 = 50;

/// A window running `run` units (at most the grant) under full retention:
/// the batch peaks at the larger of what is kept and its own cost, and keeps
/// it. `kept` is the growth above load. Returns the grant.
fn retained_window(
    handle: &TelemetryHandle,
    admission: &Admission,
    run: u64,
    kept: &mut u64,
) -> Grant {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let grant = *token.grant();
    let run = run.min(grant.unit_budget);
    *kept = (*kept).max(RETAINED_INIT_MB + RETAINED_PER_UNIT_MB * run);
    let resident = RSS_AT_LOAD_MB + *kept;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![ram_batch(run, resident, resident)]);
    token.finish(WindowOutcome::Responded { oom: None });
    grant
}

/// Batches that ran below the size already kept peak at the kept level, not
/// at their own cost; counted, they would read as a large fixed part and a
/// small per-unit cost, and the next large grant would be booked far below
/// what it needs.
#[test]
fn batches_run_in_kept_memory_say_nothing_about_the_cost() {
    let ledger = host_with_ram(&[GPU], None, 256 * 1024);
    let (handle, admission) = gpu_replica(&ledger, "g/plateau", GPU, 41);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 150_000);
    let mut kept = 0;
    for run in [41, 82, 164, 328, 656, 334, 201, 454, 82, 476, 665] {
        assert_eq!(
            retained_window(&handle, &admission, run, &mut kept)
                .unit_budget
                .min(run),
            run
        );
    }
    // 24 750 MiB truly free beside the 35 250 kept.
    ledger.record_free_for_test(cpu::DEVICE_KEY, 24_750);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let need = RETAINED_INIT_MB + RETAINED_PER_UNIT_MB * token.grant().unit_budget;
    let booked = row(&ledger, "g/plateau").ram_booked_mb;
    assert_eq!(booked, need, "booked exactly what it needs, no more");
    assert!(
        need <= kept + 24_750,
        "{need} MiB is more than is kept and free"
    );
}

/// The same holds inside one window: a batch running in memory an earlier
/// batch of the window kept is dropped.
#[test]
fn a_batch_in_memory_its_window_kept_is_dropped() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/in-window", GPU, 64);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let resident = RSS_AT_LOAD_MB + 3_200;
    handle.lock().unwrap().record_measurements(vec![
        ram_batch(64, resident, resident),
        ram_batch(40, resident, resident),
    ]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(row(&ledger, "g/in-window").ram_mb_per_unit, Some(50.0));
}

/// Seeded runs of short and full windows, with other processes' usage moving
/// under a worker that keeps what it peaked at: once the cost is measured,
/// no grant is booked below what it needs.
#[test]
fn a_retaining_worker_is_never_under_booked() {
    use rand::{Rng, SeedableRng, rngs::StdRng};
    for seed in 1..=4u64 {
        let model = format!("g/retain-{seed}");
        let ledger = host(&[GPU], None);
        let (handle, admission) = gpu_replica(&ledger, &model, GPU, 16);
        let mut rng = StdRng::seed_from_u64(seed);
        let mut kept = 0;
        for window in 0..40 {
            let others: u64 = rng.random_range(5_000..25_000);
            ledger.record_free_for_test(
                cpu::DEVICE_KEY,
                CPU_RAM_MB.saturating_sub(others + RSS_AT_LOAD_MB + kept),
            );
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let units = token.grant().unit_budget;
            let booked = row(&ledger, &model).ram_booked_mb;
            if booked > 0 {
                assert!(
                    booked >= RETAINED_INIT_MB + RETAINED_PER_UNIT_MB * units,
                    "seed {seed}, window {window}: {units} units booked at {booked} MiB"
                );
            }
            drop(token);
            let run = if rng.random_bool(0.4) {
                rng.random_range(1..=units)
            } else {
                units
            };
            retained_window(&handle, &admission, run, &mut kept);
        }
    }
}

/// A resident set that falls below its load level (load-time memory
/// released) lowers the baseline, so later batches still read their own
/// cost and the replica leaves its seed.
#[test]
fn a_resident_set_below_its_load_level_lowers_the_baseline() {
    const RELEASED: u64 = 1_500;
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/released", GPU, 64);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let mut grants = Vec::new();
    for _ in 0..8 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget;
        let low = RSS_AT_LOAD_MB - RELEASED;
        handle.lock().unwrap().record_measurements(vec![ram_batch(
            units,
            low + RAM_PER_UNIT_MB * units,
            low,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });
        grants.push(units);
    }
    assert_eq!(grants, [64, 64, 128, 256, 512, 1_024, 2_048, 4_096]);
    assert_eq!(
        row(&ledger, "g/released").ram_mb_per_unit,
        Some(RAM_PER_UNIT_MB as f64)
    );
}

/// A cheaper window at a size already measured does not replace the
/// costlier one: the booking still covers the costliest input measured.
#[test]
fn the_costliest_batch_at_a_size_is_kept() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/costliest", GPU, 64);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    for (run, per_unit) in [(64, 55), (128, 55), (64, 45), (128, 45)] {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let peak = RSS_AT_LOAD_MB + per_unit * run;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![ram_batch(run, peak, RSS_AT_LOAD_MB)]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    assert_eq!(row(&ledger, "g/costliest").ram_mb_per_unit, Some(55.0));
}

/// A failed host read is not retried at every grant: it backs off like the
/// probe.
#[test]
fn a_failed_host_read_backs_off() {
    let ledger = host(&[GPU], None);
    let (_handle, admission) = gpu_replica(&ledger, "g/unreadable", GPU, 8);
    ledger.install_probe_stub(None);
    drop(admission.request_grant(u64::MAX, None, 1, 0));
    drop(admission.request_grant(u64::MAX, None, 1, 0));
    assert_eq!(ledger.probe_calls(), 1);
}

/// Until a batch measured a GPU replica's host RAM its grant is held at the
/// model's starting size and books nothing, even where a profile lets the
/// GPU side start far higher.
#[test]
fn an_unmeasured_ram_cost_holds_the_grant_at_the_seed() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(1_024, true)),
        ..FakeProfiles::default()
    });
    let ledger = host(&[GPU], Some(profiles));
    let (handle, admission) = gpu_replica(&ledger, "g/unmeasured", GPU, 8);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);

    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().unit_budget, 8);
    let first = row(&ledger, "g/unmeasured");
    assert_eq!((first.ram_mb_per_unit, first.ram_booked_mb), (None, 0));
    assert!(first.ram_ceiling_binding);
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            peak_rss_mb: Some(RSS_AT_LOAD_MB + 80),
            rss_after_mb: Some(RSS_AT_LOAD_MB),
            ..measurement(8, 0, 180)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });

    assert_eq!(
        ram_window(&handle, &admission).unit_budget,
        1_024,
        "the profile's anchor"
    );
}

/// Only a replica on a GPU with its own memory books host RAM: a CPU
/// replica's resident set is its device memory, an MPS replica's memory is
/// RAM already, and a host without a CPU device has nothing to book on.
#[test]
fn only_a_private_memory_gpu_replica_books_host_ram() {
    let mixed = host(&[GPU], None);
    let cpu_handle = with_rss(loaded_on_cpu(Some(CPU_RAM_MB)));
    let _cpu = mixed
        .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
        .expect("admitted on RAM");
    let mac = VramLedger::new(
        &GpuInventory::known_mps(MAC_RAM_MB),
        no_margin().into(),
        None,
    );
    let mps_handle = with_rss(loaded_mps(Some(MAC_RAM_MB / 4 * 3)));
    let _mps = mac
        .register_worker("g/mps", item_cost(4), &mps_handle, Some(MPS_GPU))
        .expect("admitted on Metal");
    let bare = ledger(200_000, no_margin());
    let gpu_handle = with_rss(loaded(Some(1000), Some(0)));
    let _gpu = bare
        .register_worker("g/bare", item_cost(4), &gpu_handle, None)
        .expect("admitted");
    for (ledger, model) in [(&mixed, "g/cpu"), (&mac, "g/mps"), (&bare, "g/bare")] {
        assert_eq!(row(ledger, model).ram_resident_mb, None, "{model}");
    }
}
