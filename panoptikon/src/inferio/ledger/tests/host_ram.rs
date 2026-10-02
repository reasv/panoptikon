//! A GPU replica's host RAM, booked on the CPU device beside its GPU memory.
use super::*;
use crate::inferio::gpu::GpuInventory;
use crate::inferio::ledger::health::LedgerWorkerHealth;

/// The resident set of every GPU replica here at load, and the host RAM
/// each unit of its batches costs above it.
const RSS_AT_LOAD_MB: u64 = 2_000;
const RAM_PER_UNIT_MB: u64 = 10;

/// The CPU device's reserve on a [`CPU_RAM_MB`] host: a tenth of its RAM.
const RESERVE_MB: u64 = CPU_RAM_MB / 10;

/// A CPU free reading that leaves `mb` MiB to book beyond the reserve.
fn cpu_free_to_book(ledger: &VramLedger, mb: u64) {
    ledger.record_free_for_test(cpu::DEVICE_KEY, mb + RESERVE_MB);
}

/// Throughput that keeps rising with batch size, so the ramp never holds.
const RISING: [(u64, f64); 2] = [(1, 100.0), (1 << 20, 2_100.0)];

/// CUDA cards beside the CPU device, as `VramLedger::new` builds a GPU host.
/// With no margin, a CPU free reading of `n` MiB leaves `n − RESERVE_MB` to
/// book while every resident set is at its load baseline. The host is not
/// probed: free readings are the test's own, or a probe stub's.
fn host(cards: &[&str], profiles: Option<Arc<FakeProfiles>>) -> Arc<VramLedger> {
    host_with_ram(cards, profiles, CPU_RAM_MB)
}

fn host_with_ram(
    cards: &[&str],
    profiles: Option<Arc<FakeProfiles>>,
    ram_mb: u64,
) -> Arc<VramLedger> {
    host_with_cards(cards, 200_000, profiles, ram_mb)
}

/// [`host`] with cards of `card_mb` and `ram_mb` of host RAM.
fn host_with_cards(
    cards: &[&str],
    card_mb: u64,
    profiles: Option<Arc<FakeProfiles>>,
    ram_mb: u64,
) -> Arc<VramLedger> {
    let inventory = GpuInventory::known(
        cards
            .iter()
            .enumerate()
            .map(|(index, uuid)| nvidia(index as u32, uuid, "TEST 9000", card_mb))
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

/// A GPU replica just loaded: its first window is a single-item window.
fn cold_gpu_replica(
    ledger: &Arc<VramLedger>,
    model: &str,
    gpu: &str,
    cost: CostDimension,
) -> (TelemetryHandle, Admission) {
    let handle = with_rss(loaded_on(gpu, Some(1000), Some(0)));
    let admission = ledger
        .register_worker(model, cost, &handle, Some(gpu))
        .expect("admitted on its card");
    push_memory(&handle, 190_000, 1000);
    (handle, admission)
}

/// A GPU replica whose first two windows measured [`RAM_PER_UNIT_MB`] per
/// unit ([`measure_ram_cost`]).
fn gpu_replica(
    ledger: &Arc<VramLedger>,
    model: &str,
    gpu: &str,
    seed: u32,
) -> (TelemetryHandle, Admission) {
    let (handle, admission) = cold_gpu_replica(ledger, model, gpu, item_cost(seed));
    measure_ram_cost(&handle, &admission, 0, RAM_PER_UNIT_MB);
    forget_gpu_side(ledger, model, gpu);
    (handle, admission)
}

/// Drops what a replica's windows so far taught the GPU side (fit, anchor,
/// throughput ring, ramp), so a test of host RAM starts its ramp at the seed.
fn forget_gpu_side(ledger: &Arc<VramLedger>, model: &str, gpu: &str) {
    let mut state = ledger.lock();
    let ram = state
        .calibration
        .remove(&(model.to_owned(), gpu.to_owned()))
        .expect("measured");
    state.calibration.insert(
        (model.to_owned(), gpu.to_owned()),
        ModelCalibration {
            seeded: ram.seeded,
            ram_samples: ram.ram_samples,
            ram_cost: ram.ram_cost,
            ram_startup_mb: ram.ram_startup_mb,
            ram_first_units: ram.ram_first_units,
            ..ModelCalibration::default()
        },
    );
    for entry in state.workers.values_mut() {
        if entry.inference_id == model && entry.gpu == gpu {
            entry.clean_windows = 0;
            entry.settled_windows = 0;
            entry.ran_batches = 0;
        }
    }
}

/// A cold replica's item-capped windows, one item, then two, then four (as
/// the seed allows), each batch peaking `fixed_mb + per_unit_mb` per unit
/// over load and keeping nothing. The first batch is start-up; two sizes
/// after it measure the cost exactly.
fn measure_ram_cost(
    handle: &TelemetryHandle,
    admission: &Admission,
    fixed_mb: u64,
    per_unit_mb: u64,
) {
    single_item_window(handle, admission, 1, fixed_mb + per_unit_mb);
    while item_bound(admission) != usize::MAX {
        let items = item_bound(admission) as u64;
        let token = admission.request_grant(items, None, 1, 0).expect("granted");
        assert_eq!(token.grant().user_cap_items, Some(items as u32));
        handle.lock().unwrap().record_measurements(vec![ram_batch(
            items,
            RSS_AT_LOAD_MB + fixed_mb + items * per_unit_mb,
            RSS_AT_LOAD_MB,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
}

/// The items per batch the replica is capped at; `usize::MAX` if uncapped.
fn item_bound(admission: &Admission) -> usize {
    admission.item_cap().map_or(usize::MAX, |cap| cap as usize)
}

/// A single-item window of `units`: one batch of one item that grows the
/// resident set by `growth_mb` and hands it back after. Returns the grant.
fn single_item_window(
    handle: &TelemetryHandle,
    admission: &Admission,
    units: u64,
    growth_mb: u64,
) -> Grant {
    assert_eq!(admission.window_item_bound(), 1, "one item in the window");
    let token = admission.request_grant(units, None, 1, 0).expect("granted");
    let grant = *token.grant();
    assert_eq!(grant.user_cap_items, Some(1), "one item per batch");
    handle.lock().unwrap().record_measurements(vec![ram_batch(
        units,
        RSS_AT_LOAD_MB + growth_mb,
        RSS_AT_LOAD_MB,
    )]);
    token.finish(WindowOutcome::Responded { oom: None });
    grant
}

/// [`ramp_window`] at [`RISING`] from a worker that reports its host RAM:
/// every batch peaks [`RAM_PER_UNIT_MB`] per unit over [`RSS_AT_LOAD_MB`]
/// and hands it back after. Every size memory did not cut earns the next
/// ([`Admission::earn_next_size`]). Returns the grant.
fn ram_window(handle: &TelemetryHandle, admission: &Admission) -> Grant {
    ram_window_costing(handle, admission, 0, RAM_PER_UNIT_MB)
}

/// [`ram_window`] with batches that peak `fixed_mb + per_unit_mb` per unit.
fn ram_window_costing(
    handle: &TelemetryHandle,
    admission: &Admission,
    fixed_mb: u64,
    per_unit_mb: u64,
) -> Grant {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let grant = *token.grant();
    let units = grant.unit_budget;
    let rate = ladder_rate(&RISING, units);
    let host_ram = |batch: BatchMeasurement| BatchMeasurement {
        peak_rss_mb: Some(RSS_AT_LOAD_MB + fixed_mb + per_unit_mb * units),
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
    if !grant.squeezed {
        admission.earn_next_size();
    }
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
    admission.earn_next_size();
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

/// The batch size state a RAM ceiling must leave alone.
fn ramp_state(row: &LedgerWorkerHealth) -> (u32, Option<u64>, Option<u64>) {
    (row.deflation, row.knee_units, row.trial_units)
}

/// The one replica's window counters: settled and clean windows, batches run.
fn window_counts(ledger: &Arc<VramLedger>) -> (u64, u32, u64) {
    let state = ledger.lock();
    let entry = state.workers.values().next().expect("one replica");
    (
        entry.settled_windows,
        entry.clean_windows,
        entry.ran_batches,
    )
}

/// With RAM to spare and its cost measured, a replica that books it is
/// granted exactly what one on a host without a CPU device is, window after
/// window, from the same GPU-side state.
#[test]
fn plentiful_host_ram_changes_no_grant() {
    let base = ledger(200_000, no_margin());
    let base_handle = loaded(Some(1000), Some(0));
    let base_admission = base
        .register_worker("g/plenty", item_cost(8), &base_handle, None)
        .expect("admitted");
    push_memory(&base_handle, 190_000, 1000);
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/plenty", GPU, item_cost(8));
    cpu_free_to_book(&ledger, 45_000);
    measure_ram_cost(&handle, &admission, 0, RAM_PER_UNIT_MB);
    forget_gpu_side(&ledger, "g/plenty", GPU);

    for window in 0..10 {
        let (booked, unbooked) = (row(&ledger, "g/plenty"), row(&base, "g/plenty"));
        assert_eq!(
            ramp_state(&booked),
            ramp_state(&unbooked),
            "window {window}"
        );
        assert_eq!(booked.max_units_measured, unbooked.max_units_measured);
        assert_eq!(window_counts(&ledger), window_counts(&base));
        let expected = ram_window(&base_handle, &base_admission);
        let granted = ram_window(&handle, &admission);
        assert_eq!(granted.ram_mb, RAM_PER_UNIT_MB * granted.unit_budget);
        assert_eq!(granted.ram_reserve_mb, RESERVE_MB);
        let gpu_side = Grant {
            ram_mb: 0,
            ram_reserve_mb: 0,
            ..granted
        };
        assert_eq!(gpu_side, expected, "window {window}");
    }
    let booked = row(&ledger, "g/plenty");
    assert_eq!(booked.unit_budget, 8 << 10, "still ramping");
    assert_eq!(booked.ram_resident_mb, Some(RSS_AT_LOAD_MB));
    assert_eq!(booked.ram_mb_per_unit, Some(RAM_PER_UNIT_MB as f64));
    assert_eq!(booked.ram_booked_mb, 0, "no grant outstanding");
    assert!(!booked.ram_ceiling_binding);
}

/// When host RAM gets tight mid-job the next grant is what it holds, like
/// the edge of a full card: the batch size keeps its place, the throughput
/// ring takes the batches at the size they ran and nothing deflates. When
/// RAM frees up the batch size resumes where it stood.
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
    cpu_free_to_book(&ledger, 3_000);
    let logs = captured_logs(|| {
        for _ in 0..3 {
            let grant = ram_window(&handle, &admission);
            assert_eq!(grant.unit_budget, 300);
            assert_eq!(grant.mb, 3_100, "the GPU reserves only what 300 units need");
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
    assert!(after.throughput_samples > before.throughput_samples);
    assert_eq!(after.max_units_measured, 300, "a clean batch it did run");

    // Tighter still: the next grant shrinks below the last.
    cpu_free_to_book(&ledger, 1_000);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 100);

    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 384);
    assert!(!row(&ledger, "g/capped").ram_ceiling_binding);
}

/// A window host RAM cut below the working size did not run at it: it starts
/// no trial of a larger one and leaves the working size where it is.
#[test]
fn a_ram_capped_window_starts_no_trial() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/knee", GPU, 64);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    ram_window(&handle, &admission);
    ledger.set_knee_for_test("g/knee", GPU, 100);
    cpu_free_to_book(&ledger, 500);
    for _ in 0..RETEST_WINDOWS {
        assert_eq!(ram_window(&handle, &admission).unit_budget, 50);
    }
    assert_eq!(ledger.trial_for_test("g/knee", GPU).0, None);
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
    cpu_free_to_book(&ledger, 3_000);
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

/// A GPU replica that books host RAM counts as a replica on the CPU device:
/// a pre-fit CPU replica reserves half the RAM headroom, and the GPU
/// replica's batch is not capped at one unit.
#[test]
fn a_pre_fit_cpu_replica_leaves_host_ram_for_a_gpu_replicas_batch() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/on-gpu", GPU, 256);
    let cpu_handle = loaded_on_cpu(Some(CPU_RAM_MB));
    let on_cpu = ledger
        .register_worker("g/on-cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
        .expect("admitted on RAM");
    // Nothing else holds RAM: the resident set and the base are ours.
    ledger.record_free_for_test(cpu::DEVICE_KEY, CPU_RAM_MB - RSS_AT_LOAD_MB - 1000);
    let headroom = ledger.headroom_mb(cpu::DEVICE_KEY);

    let held = on_cpu.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(held.grant().mb, headroom / 2);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 256);
}

/// A replica alone on its card reserves the card's whole headroom, whatever
/// is on the other card: a replica that books host RAM, and a load in flight.
#[tokio::test]
async fn a_replica_alone_on_its_card_is_not_cut_for_another_cards_replicas() {
    const OTHER: &str = "GPU-bbbb";
    let ledger = host(&[GPU, OTHER], None);
    let mut admissions = Vec::new();
    for (model, card) in [("g/alone", GPU), ("g/other", OTHER)] {
        let handle = with_rss(loaded_on(card, Some(1000), Some(0)));
        let admission = ledger
            .register_worker(model, item_cost(4), &handle, Some(card))
            .expect("admitted on its card");
        push_memory(&handle, 199_000, 0);
        admissions.push(admission);
    }
    ledger.ingest_all_for_test();
    let _loading = ledger
        .reserve_load_for_test("g/loading", item_cost(4), OTHER, None)
        .await
        .expect("known GPU");

    let token = admissions[0]
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().mb, 199_000);
}

/// A GPU replica's host RAM booking is a reservation on the CPU device: a
/// cold CPU replica asking beside it is cut to what is left, here one unit.
#[test]
fn a_gpu_replicas_ram_booking_cuts_a_cold_cpu_replicas_batch() {
    // 8080 MiB of RAM: a 6060 MiB limit under the shipped cap.
    const RAM_MB: u64 = 8080;
    let ledger = host_with_ram(&[GPU], None, RAM_MB);
    let (_handle, admission) = gpu_replica(&ledger, "g/on-gpu", GPU, 256);
    let cpu_handle = loaded_on_cpu(Some(RAM_MB));
    let on_cpu = ledger
        .register_worker("g/on-cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
        .expect("admitted on RAM");
    // Nothing else holds RAM: the resident set and the base are ours.
    ledger.record_free_for_test(cpu::DEVICE_KEY, RAM_MB - RSS_AT_LOAD_MB - 1000);
    assert_eq!(ledger.headroom_mb(cpu::DEVICE_KEY), 3060);

    let booked = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(booked.grant().unit_budget, 256);
    assert_eq!(row(&ledger, "g/on-gpu").ram_booked_mb, 2560);
    let cut = on_cpu.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!((cut.grant().mb, cut.grant().unit_budget), (500, 1));
    assert!(cut.grant().squeezed);
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
    assert_eq!(
        cpu.headroom_mb,
        CPU_RAM_MB - OTHERS - RESERVE_MB - RSS_AT_LOAD_MB
    );

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
    cpu_free_to_book(&ledger, 8_000);
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

/// One-time host growth on the first batch (CUDA and library start-up)
/// joins the load level, so a model cheap per unit reaches the ceiling its
/// RAM allows rather than one priced as if every unit carried that growth.
#[test]
fn one_time_growth_does_not_hold_a_cheap_model_down() {
    const INIT: u64 = 1_500;
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/cheap", GPU, item_cost(64));
    cpu_free_to_book(&ledger, 3_000);
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
        admission.earn_next_size();
    }
    // 3 000 MiB free before start-up took 1 500: 1 500 units at 1 MiB each.
    assert_eq!(last, 1_500);
    assert!(row(&ledger, "g/cheap").ram_ceiling_binding);
    let _token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        row(&ledger, "g/cheap").ram_booked_mb,
        1_500,
        "the start-up is resident, not booked"
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
    cpu_free_to_book(&ledger, 6_000);
    assert_eq!(kept_window(&a_handle, &a), 512);

    // 880 MiB truly free; A may also reuse the 5 120 it kept.
    let b_grant = b.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(b_grant.grant().unit_budget, 88);
    let a_grant = a.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(a_grant.grant().unit_budget, 512);
}

/// A GPU replica that dies while its window holds a host RAM booking is
/// capped at half that batch, since host RAM may be what killed it. Its
/// anchor and ramp are left alone: on private memory a death is no negative.
#[test]
fn a_gpu_replica_that_dies_with_host_ram_booked_is_capped() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/died", GPU, 64);
    cpu_free_to_book(&ledger, 45_000);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 64);
    assert_eq!(ram_window(&handle, &admission).unit_budget, 128);
    let before = row(&ledger, "g/died");

    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().unit_budget, 256);
    assert_eq!(before.ram_booked_mb, 0);
    assert_eq!(row(&ledger, "g/died").ram_booked_mb, 2_560);
    token.finish(WindowOutcome::WorkerDied);

    let after = row(&ledger, "g/died");
    assert_eq!(after.death_cap_units, Some(128));
    assert_eq!(after.unit_budget, 128);
    assert_eq!(after.max_units_measured, before.max_units_measured);
    assert_eq!((after.deflation, after.knee_units), (0, before.knee_units));
    for _ in 0..4 {
        assert_eq!(ram_window(&handle, &admission).unit_budget, 128);
    }
}

/// An item-capped window is sized by the cap, not by the queue, even when the
/// dispatcher hands it exactly that many items: its death still caps.
#[test]
fn a_death_in_a_booked_item_capped_window_caps() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/capped", GPU, item_cost(64));
    cpu_free_to_book(&ledger, 45_000);
    single_item_window(&handle, &admission, 1, RAM_PER_UNIT_MB);
    // Two items: the first size measured, so the next window is booked.
    let token = admission.request_grant(2, None, 1, 0).expect("granted");
    handle.lock().unwrap().record_measurements(vec![ram_batch(
        2,
        RSS_AT_LOAD_MB + 2 * RAM_PER_UNIT_MB,
        RSS_AT_LOAD_MB,
    )]);
    token.finish(WindowOutcome::Responded { oom: None });
    admission.earn_next_size();

    assert_eq!(item_bound(&admission), 4);
    let token = admission.request_grant(4, None, 1, 0).expect("granted");
    assert_eq!(token.grant().user_cap_items, Some(4));
    assert!(row(&ledger, "g/capped").ram_booked_mb > 0);
    token.finish(WindowOutcome::WorkerDied);
    assert_eq!(row(&ledger, "g/capped").death_cap_units, Some(2));
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
    host_ram_free(&ledger, 3_000 + RESERVE_MB);
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

    // 1 000 MiB free beside it: 1 500 MiB to book, 150 units, of which the
    // worker's clamp is told only the 1 000 that free RAM must supply.
    cpu_free_to_book(&ledger, 1_000);
    assert_eq!(cpu_row(&ledger).footprints_mb, RSS_AT_LOAD_MB + KEPT);
    let grant = ram_window(&handle, &admission);
    assert_eq!((grant.unit_budget, grant.ram_mb), (150, 1_000));
    assert_eq!(grant.ram_reserve_mb, RESERVE_MB);
}

/// A batch books the fixed part the fit separates once two sizes ran, plus
/// per unit the largest cost above it among batches within twice the
/// largest. A cost from one size, or with no rising slope, is not fitted.
#[test]
fn the_ram_cost_is_the_fixed_part_plus_an_upper_per_unit_cost() {
    let cost = |samples: &[(u64, u64)]| {
        let ring: Vec<FitSample> = samples
            .iter()
            .map(|&(units, delta_mb)| FitSample { units, delta_mb })
            .collect();
        ram_cost(&ring, 0, 0).map(|cost| {
            (
                cost.fixed_mb,
                cost.mb_per_unit,
                cost.fitted,
                cost.measured_units,
            )
        })
    };
    assert_eq!(cost(&[]), None);
    assert_eq!(
        cost(&[(8, 280)]),
        Some((0.0, 35.0, false, 8)),
        "one size: the fixed part is priced per unit"
    );
    assert_eq!(cost(&[(16, 360), (8, 280)]), Some((200.0, 10.0, true, 16)));
    assert_eq!(
        cost(&[(16, 360), (32, 520), (48, 1_400), (64, 840)]),
        Some((200.0, 25.0, true, 64)),
        "a batch of costly inputs raises the per-unit cost"
    );
    assert_eq!(
        cost(&[(8, 1_000), (32, 520), (64, 840)]),
        Some((0.0, 16.25, false, 64)),
        "the 8-unit batch, reading memory kept from a larger one, is too small to count"
    );
    assert_eq!(cost(&[(8, 0)]), None, "0 is unknown, not free");
    assert_eq!(
        cost(&[(10, 50), (20, 150), (40, 350)]),
        Some((0.0, 10.0, true, 40)),
        "a negative fixed part is 0"
    );
}

/// A fit from small batches prices at most twice the largest at its fitted
/// figures. A larger batch books the measured batches' whole growth per
/// unit, so a per-unit cost the fit read as fixed is not left out.
#[test]
fn a_fit_from_small_batches_does_not_price_a_far_larger_batch() {
    let sample = |units, delta_mb| FitSample { units, delta_mb };
    // Two and four items after a first batch of one: 223 MiB + 40 per unit.
    let small = [sample(2, 303), sample(4, 383)];
    let cost = ram_cost(&small, 1, 0).expect("a cost");
    assert_eq!((cost.fixed_mb, cost.mb_per_unit), (223.0, 40.0));
    assert_eq!((cost.fitted, cost.fitted_reach()), (true, 8));
    assert_eq!(cost.whole_mb_per_unit, 151.5, "303 MiB over 2 units");
    assert_eq!(cost.booking_mb(8), 223 + 8 * 40);
    // 192 units that grow 9 700 MiB: 7 903 at the fitted figures.
    assert_eq!(cost.booking_mb(192), 223 + 29_088);
    // 10 000 MiB of room holds 64 units at the whole rate, not the 244 the
    // fitted figures would admit; little room still holds the fitted reach.
    assert_eq!(cost.units_within(10_000.0), 64);
    assert_eq!(cost.units_within(700.0), 8);
    assert_eq!(cost.units_within(100.0), 1);

    // Measured at 192, the fitted figures reach 384.
    let grown = [sample(2, 303), sample(4, 383), sample(192, 9_923)];
    let cost = ram_cost(&grown, 1, 0).expect("a cost");
    assert_eq!(cost.fitted_reach(), 384);
    assert_eq!(
        cost.booking_mb(384),
        (cost.fixed_mb + 384.0 * cost.mb_per_unit).ceil() as u64
    );
    assert!(cost.booking_mb(385) > 385 * 51);

    // One size: the fixed part is already priced per unit.
    let one = ram_cost(&[sample(8, 280)], 0, 0).expect("a cost");
    assert_eq!(one.whole_mb_per_unit, one.mb_per_unit);
}

/// Under tight host RAM the batch still grows window by window to what the
/// RAM holds: each window is priced from the largest batch measured so far,
/// and none books less than it uses.
#[test]
fn a_cost_measured_at_small_batches_does_not_stall_the_ramp() {
    const FIXED: u64 = 200;
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/far", GPU, item_cost(64));
    measure_ram_cost(&handle, &admission, FIXED, RAM_PER_UNIT_MB);
    forget_gpu_side(&ledger, "g/far", GPU);
    // 3 000 MiB to book: 280 units at 200 MiB + 10 per unit.
    cpu_free_to_book(&ledger, 3_000);
    let mut sizes = Vec::new();
    for _ in 0..6 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget;
        let booked = row(&ledger, "g/far").ram_booked_mb;
        assert!(booked >= FIXED + RAM_PER_UNIT_MB * units, "{units} units");
        assert!(booked <= 3_000, "{units} units booked {booked}");
        drop(token);
        sizes.push(ram_window_costing(&handle, &admission, FIXED, RAM_PER_UNIT_MB).unit_budget);
    }
    // 25 at the whole rate, then doubling (the ratchet over the largest
    // batch run) up to what the RAM holds.
    assert_eq!(sizes, [25, 50, 100, 200, 280, 280]);
}

/// Start-up growth and cost per unit of the retention tests' model.
const RETAINED_INIT_MB: u64 = 2_000;
const RETAINED_PER_UNIT_MB: u64 = 50;

/// A window running `run` units (at most the grant): the batch peaks at the
/// larger of what is kept and its own cost. With `retain` the worker keeps
/// all of it (glibc without trimming), otherwise only the start-up growth.
/// `kept` is the growth above load. Returns the grant.
fn retained_window(
    handle: &TelemetryHandle,
    admission: &Admission,
    run: u64,
    kept: &mut u64,
    retain: bool,
) -> Grant {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let grant = *token.grant();
    let run = run
        .min(grant.unit_budget)
        .min(grant.user_cap_items.map_or(u64::MAX, u64::from));
    let peak = (*kept).max(RETAINED_INIT_MB + RETAINED_PER_UNIT_MB * run);
    *kept = if retain { peak } else { RETAINED_INIT_MB };
    handle.lock().unwrap().record_measurements(vec![ram_batch(
        run,
        RSS_AT_LOAD_MB + peak,
        RSS_AT_LOAD_MB + *kept,
    )]);
    token.finish(WindowOutcome::Responded { oom: None });
    admission.earn_next_size();
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
            retained_window(&handle, &admission, run, &mut kept, true)
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
    let (handle, admission) = cold_gpu_replica(&ledger, "g/in-window", GPU, item_cost(64));
    measure_ram_cost(&handle, &admission, 0, 50);
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

/// Seeded runs of short and full windows, with other processes' usage moving,
/// under a worker that keeps what it peaked at from its first batch on, or
/// only its start-up: once the cost is measured, no grant books less than
/// its batch will add to the resident set.
#[test]
fn a_retaining_worker_is_never_under_booked() {
    use rand::{Rng, SeedableRng, rngs::StdRng};
    for (seed, retain) in (1..=8u64).map(|seed| (seed, seed % 2 == 1)) {
        let model = format!("g/retain-{seed}");
        let ledger = host(&[GPU], None);
        let (handle, admission) = cold_gpu_replica(&ledger, &model, GPU, item_cost(16));
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
            let need = (RETAINED_INIT_MB + RETAINED_PER_UNIT_MB * units).saturating_sub(kept);
            if booked > 0 {
                assert!(
                    booked >= need,
                    "seed {seed}, retain {retain}, window {window}: {units} units \
                     booked at {booked} MiB, need {need}"
                );
            }
            drop(token);
            let run = if rng.random_bool(0.4) {
                rng.random_range(1..=units)
            } else {
                units
            };
            retained_window(&handle, &admission, run, &mut kept, retain);
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
    cpu_free_to_book(&ledger, 45_000);
    let mut grants = Vec::new();
    for _ in 0..7 {
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
        admission.earn_next_size();
        grants.push(units);
    }
    assert_eq!(grants, [64, 128, 256, 512, 1_024, 2_048, 4_096]);
    assert_eq!(
        row(&ledger, "g/released").ram_mb_per_unit,
        Some(RAM_PER_UNIT_MB as f64)
    );
}

/// A resident set that dips below its load level and comes back with the
/// next batch (pages reclaimed under pressure) lowers the sample baseline but
/// not the replica's own credit: no grant needs more new RAM than is free.
#[test]
fn a_transient_dip_below_the_load_level_does_not_over_commit() {
    const INIT: u64 = 1_000;
    /// Host RAM free beyond the replica's resident set at load.
    const BEYOND_LOAD: u64 = 8_000;
    for dip in [500, 1_500] {
        let model = format!("g/dip-{dip}");
        let ledger = host(&[GPU], None);
        let (handle, admission) = gpu_replica(&ledger, &model, GPU, 64);
        let mut resident = RSS_AT_LOAD_MB;
        for window in 0..12 {
            let free = RSS_AT_LOAD_MB + BEYOND_LOAD - resident;
            ledger.record_free_for_test(cpu::DEVICE_KEY, free);
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let units = token.grant().unit_budget;
            let peak = RSS_AT_LOAD_MB + INIT + RAM_PER_UNIT_MB * units;
            // From the third window on, three sizes (the setup's item
            // included) have measured the cost.
            if window >= 2 {
                assert!(
                    peak - resident <= free,
                    "dip {dip}, window {window}: {units} units need {} MiB, {free} free",
                    peak - resident
                );
            }
            let after = RSS_AT_LOAD_MB + INIT - if window == 4 { dip } else { 0 };
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![ram_batch(units, peak, after)]);
            token.finish(WindowOutcome::Responded { oom: None });
            resident = after;
        }
    }
}

/// A cheaper window at a size already measured does not replace the
/// costlier one: the booking still covers the costliest input measured.
#[test]
fn the_costliest_batch_at_a_size_is_kept() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/costliest", GPU, item_cost(64));
    measure_ram_cost(&handle, &admission, 0, 55);
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

/// While its batches grow no host RAM the cost stays unknown: each window
/// doubles the items per batch (1, 2, 4, …, the user's cap still applying),
/// books nothing and keeps the seed's unit budget, even where a profile lets
/// the GPU side start far higher. The first batch that grows RAM prices the
/// next window.
#[test]
fn the_item_cap_doubles_until_a_batch_grows_host_ram() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(1_024, true)),
        ..FakeProfiles::default()
    });
    let ledger = host(&[GPU], Some(profiles));
    let (handle, admission) = cold_gpu_replica(&ledger, "g/ungrown", GPU, item_cost(64));
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let mut caps = Vec::new();
    for grows in [false, false, false, true] {
        let items = item_bound(&admission);
        let token = admission.request_grant(64, Some(3), 1, 0).expect("granted");
        let grant = *token.grant();
        assert_eq!(
            grant.unit_budget,
            u64::from(grant.user_cap_items.expect("capped")),
            "one item, one unit"
        );
        let row_now = row(&ledger, "g/ungrown");
        assert_eq!(
            (row_now.ram_booked_mb, row_now.ram_ceiling_binding),
            (0, false)
        );
        let ran = u64::from(grant.user_cap_items.expect("capped"));
        caps.push((items, ran));
        let peak = RSS_AT_LOAD_MB + if grows { RAM_PER_UNIT_MB * ran } else { 0 };
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![ram_batch(ran, peak, RSS_AT_LOAD_MB)]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    assert_eq!(caps, [(1, 1), (2, 2), (4, 3), (8, 3)]);
    // One size measured: the cap goes on, now booked and at most twice that
    // size, until a second size.
    assert_eq!(item_bound(&admission), 16);
    let capped = ram_window(&handle, &admission);
    assert_eq!((capped.unit_budget, capped.user_cap_items), (6, Some(16)));
    assert_eq!(item_bound(&admission), usize::MAX);
    let next = ram_window(&handle, &admission);
    assert_eq!((next.unit_budget, next.user_cap_items), (1_024, None));
    assert_eq!(row(&ledger, "g/ungrown").max_units_measured, 1_024);
}

/// A replica's first window after load runs one item and books nothing; what
/// it keeps is start-up memory, in the load level, not a cost. The next runs
/// two items, still unbooked, and prices the one after, which the ramp sizes
/// from the profile's anchor, not from the capped windows.
#[test]
fn the_first_window_after_load_is_a_single_item() {
    const INIT: u64 = 700;
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(1_024, true)),
        ..FakeProfiles::default()
    });
    let ledger = host(&[GPU], Some(profiles));
    let (handle, admission) = cold_gpu_replica(&ledger, "g/first", GPU, item_cost(8));
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let before = row(&ledger, "g/first");

    assert_eq!(item_bound(&admission), 1);
    // One request of 64 items with a batch cap of 64: batches of one, its GPU
    // reservation the seed's.
    let token = admission
        .request_grant(64, Some(64), 1, 0)
        .expect("granted");
    assert_eq!(
        (token.grant().unit_budget, token.grant().user_cap_items),
        (1, Some(1)),
        "one item, one unit"
    );
    assert!(!token.grant().squeezed, "the next window is sized as usual");
    let first = row(&ledger, "g/first");
    assert_eq!((first.ram_booked_mb, first.ram_ceiling_binding), (0, false));
    // Start-up growth stays resident after the batch.
    let resident = RSS_AT_LOAD_MB + INIT;
    handle.lock().unwrap().record_measurements(vec![ram_batch(
        1,
        resident + RAM_PER_UNIT_MB,
        resident,
    )]);
    token.finish(WindowOutcome::Responded { oom: None });
    let after = row(&ledger, "g/first");
    assert_eq!(after.ram_mb_per_unit, None, "start-up is no cost");

    assert_eq!(item_bound(&admission), 2);
    assert_eq!(
        admission.window_item_bound(),
        6,
        "three batches of two, not three of the anchor's 1 024 units"
    );
    // A window that holds less than the cap is sized by what it holds.
    let short = admission.request_grant(1, None, 1, 0).expect("granted");
    assert_eq!(
        (short.grant().unit_budget, short.grant().user_cap_items),
        (1, Some(2))
    );
    drop(short);
    let token = admission.request_grant(64, None, 1, 0).expect("granted");
    assert_eq!(token.grant().user_cap_items, Some(2));
    assert_eq!(row(&ledger, "g/first").ram_booked_mb, 0);
    handle.lock().unwrap().record_measurements(vec![ram_batch(
        2,
        resident + 2 * RAM_PER_UNIT_MB,
        resident,
    )]);
    token.finish(WindowOutcome::Responded { oom: None });
    let after = row(&ledger, "g/first");
    assert_eq!(ramp_state(&after), ramp_state(&before));
    assert_eq!(after.max_units_measured, 1_024, "the profile's anchor");
    // One size: the two items' growth over the one beyond the first.
    assert_eq!(after.ram_mb_per_unit, Some(2.0 * RAM_PER_UNIT_MB as f64));

    // One size measured: the next is item-capped still, now booked.
    assert_eq!(admission.window_item_bound(), 12);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        (token.grant().unit_budget, token.grant().user_cap_items),
        (4, Some(4))
    );
    assert_eq!(row(&ledger, "g/first").ram_booked_mb, 4 * 20);
    drop(token);
    let third = ram_window_kept(&handle, &admission, INIT);
    assert_eq!(third.unit_budget, 4);
    assert_eq!(admission.window_item_bound(), usize::MAX);
    let fourth = ram_window_kept(&handle, &admission, INIT);
    assert_eq!(fourth.unit_budget, 1_024, "the ramp's own size");
    assert_eq!(
        row(&ledger, "g/first").ram_mb_per_unit,
        Some(RAM_PER_UNIT_MB as f64)
    );
}

/// [`ram_window`]'s single batch over `kept_mb` of start-up growth that stays
/// resident; the booking is checked while the grant is held.
fn ram_window_kept(handle: &TelemetryHandle, admission: &Admission, kept_mb: u64) -> Grant {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let grant = *token.grant();
    let units = grant.unit_budget;
    let resident = RSS_AT_LOAD_MB + kept_mb;
    handle.lock().unwrap().record_measurements(vec![ram_batch(
        units,
        resident + RAM_PER_UNIT_MB * units,
        resident,
    )]);
    token.finish(WindowOutcome::Responded { oom: None });
    grant
}

/// A model whose first call loads far more than its batches use (8 900 MiB
/// of libraries and kernels, 90 MiB per unit) on a host with a few GB to
/// spare: the start-up joins the load level, so the ceiling is room / 90
/// within five windows, not held at one unit by start-up priced per unit. A
/// second one-item window (a short queue) measures nothing and does not
/// double the item cap.
#[test]
fn start_up_memory_does_not_hold_a_small_host_at_one_unit() {
    const STARTUP: u64 = 8_900;
    const PER_UNIT: u64 = 90;
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(1_024, true)),
        ..FakeProfiles::default()
    });
    let ledger = host_with_ram(&[GPU], Some(profiles), 24 * 1024);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/startup", GPU, item_cost(32));
    ledger.record_free_for_test(cpu::DEVICE_KEY, 16_000);
    let level = RSS_AT_LOAD_MB + STARTUP;
    let mut grants = Vec::new();
    for window in 0..8 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let grant = *token.grant();
        let units = grant
            .unit_budget
            .min(grant.user_cap_items.map_or(u64::MAX, u64::from))
            .min(if window == 1 { 1 } else { u64::MAX });
        let booked = row(&ledger, "g/startup").ram_booked_mb;
        if window > 3 {
            assert!(booked >= PER_UNIT * units, "window {window}: {booked} MiB");
        }
        let before = if window == 0 { RSS_AT_LOAD_MB } else { level };
        handle.lock().unwrap().record_measurements(vec![ram_batch(
            units,
            before.max(level) + PER_UNIT * units,
            level,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });
        grants.push(units);
    }
    let ceiling = ledger.headroom_mb(cpu::DEVICE_KEY) / PER_UNIT;
    assert!(ceiling > 1);
    assert_eq!(
        grants[..4],
        [1, 1, 2, 4],
        "item-capped: unbooked, then booked"
    );
    assert_eq!(grants[4..], [ceiling; 4], "{grants:?}");
    let startup = row(&ledger, "g/startup");
    assert_eq!(startup.ram_mb_per_unit, Some(PER_UNIT as f64));
    assert!(startup.ram_ceiling_binding);
}

/// On a small host a short window does not double the item cap, and a run
/// of them does not end it while the cost is unknown: no window adds more
/// than the RAM left after start-up.
#[test]
fn a_short_window_does_not_double_the_item_cap() {
    const STARTUP: u64 = 8_900;
    const PER_UNIT: u64 = 650;
    for (short, expected) in [(1, &[1, 1, 2][..]), (5, &[1, 1, 1, 1, 1, 1, 2][..])] {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(1_024, true)),
            ..FakeProfiles::default()
        });
        let ledger = host_with_ram(&[GPU], Some(profiles), 16 * 1024);
        let (handle, admission) = cold_gpu_replica(&ledger, "g/short", GPU, item_cost(32));
        ledger.record_free_for_test(cpu::DEVICE_KEY, 14_000);
        let level = RSS_AT_LOAD_MB + STARTUP;
        let mut grants = Vec::new();
        for window in 0..10 {
            let room = ledger.headroom_mb(cpu::DEVICE_KEY);
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let grant = *token.grant();
            let units = grant
                .unit_budget
                .min(grant.user_cap_items.map_or(u64::MAX, u64::from))
                .min(if window == 0 || window > short {
                    u64::MAX
                } else {
                    1
                });
            if window > 0 {
                assert!(
                    PER_UNIT * units <= room,
                    "{short} short, window {window}: {units} units, {room} MiB"
                );
            }
            let before = if window == 0 { RSS_AT_LOAD_MB } else { level };
            handle.lock().unwrap().record_measurements(vec![ram_batch(
                units,
                before.max(level) + PER_UNIT * units,
                level,
            )]);
            token.finish(WindowOutcome::Responded { oom: None });
            grants.push(units);
        }
        assert_eq!(grants[..expected.len()], *expected, "{grants:?}");
    }
}

/// Variable input cost under a worker that keeps what it peaked at: a costly
/// first page stays in the load level, and the next window's cheaper pages
/// read almost no cost. That estimate prices only an item-capped window,
/// whose shortfall is at most its own pages; the second size gives the slope,
/// and no later window books less than its pages add, none costing more than
/// one measured.
#[test]
fn a_costly_first_input_under_retention_costs_at_most_a_capped_batch() {
    const STARTUP: u64 = 700;
    const COSTLIEST: u64 = 650;
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/pages", GPU, item_cost(16));
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let mut kept = 0;
    for window in 0..8 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let grant = *token.grant();
        let pages = grant
            .unit_budget
            .min(grant.user_cap_items.map_or(u64::MAX, u64::from));
        let cost: u64 = match window {
            0 => COSTLIEST,
            1 => 300 + 360,
            _ => 460 * pages,
        };
        let peak = kept.max(STARTUP + cost);
        let need = peak - kept;
        let booked = row(&ledger, "g/pages").ram_booked_mb;
        if booked > 0 {
            let allowed = if grant.user_cap_items.is_some() {
                COSTLIEST * pages
            } else {
                0
            };
            assert!(
                booked + allowed >= need,
                "window {window}: {pages} pages booked at {booked} MiB, need {need}"
            );
        }
        kept = peak;
        handle.lock().unwrap().record_measurements(vec![ram_batch(
            pages,
            RSS_AT_LOAD_MB + peak,
            RSS_AT_LOAD_MB + kept,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });
        admission.earn_next_size();
    }
    assert!(
        row(&ledger, "g/pages").unit_budget >= 64,
        "the ramp goes on"
    );
}

/// A replica loaded after the cost is known books the start-up memory the
/// first replica's first batch kept on top of the fixed part, until its own
/// first batch has run.
#[test]
fn a_reload_books_the_start_up_its_first_batch_adds() {
    const STARTUP: u64 = 700;
    const FIXED: u64 = 100;
    let ledger = host(&[GPU], None);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let level = RSS_AT_LOAD_MB + STARTUP;
    let (handle, admission) = cold_gpu_replica(&ledger, "g/restart", GPU, item_cost(8));
    for units in [1, 2, 4] {
        let token = admission.request_grant(units, None, 1, 0).expect("granted");
        handle.lock().unwrap().record_measurements(vec![ram_batch(
            units,
            level + FIXED + RAM_PER_UNIT_MB * units,
            level,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });
        admission.earn_next_size();
    }
    // Two sizes: 100 MiB fixed, 10 per unit.
    assert_eq!(item_bound(&admission), usize::MAX);
    drop(admission);

    let (handle, admission) = cold_gpu_replica(&ledger, "g/restart", GPU, item_cost(8));
    let token = admission.request_grant(8, None, 1, 0).expect("granted");
    assert_eq!(
        row(&ledger, "g/restart").ram_booked_mb,
        FIXED + STARTUP + 8 * RAM_PER_UNIT_MB
    );
    // It keeps 80 MiB beyond the start-up: growth it may reuse, not load level.
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![ram_batch(8, level + 180, level + 80)]);
    token.finish(WindowOutcome::Responded { oom: None });
    let token = admission.request_grant(8, None, 1, 0).expect("granted");
    let booked = FIXED + 8 * RAM_PER_UNIT_MB;
    assert_eq!(row(&ledger, "g/restart").ram_booked_mb, booked, "started");
    assert_eq!(cpu_row(&ledger).charges_mb, level + 80 + (booked - 80));
    drop(token);
}

/// Two replicas starting cold at once each measure their first batch: the
/// largest start-up and first batch are what later samples and reloads use.
#[test]
fn the_largest_first_batch_of_two_cold_replicas_is_recorded() {
    let ledger = host(&[GPU], None);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let pixels = CostDimension {
        unit: CostUnit::Pixel,
        aggregation: Some(CostAggregation::Sum),
        epoch: 1,
        seed_units: Some(8_000_000),
        degraded: false,
        canvas_pixels: None,
        max_tokens: None,
    };
    let (a, a_admission) = cold_gpu_replica(&ledger, "g/twin", GPU, pixels);
    let (b, b_admission) = cold_gpu_replica(&ledger, "g/twin", GPU, pixels);
    for (handle, admission, image, kept) in [
        (&a, &a_admission, 2_000_000u64, 900u64),
        (&b, &b_admission, 1_000_000, 700),
    ] {
        let token = admission.request_grant(image, None, 1, 0).expect("granted");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                items: Some(1),
                ..ram_batch(image, RSS_AT_LOAD_MB + kept, RSS_AT_LOAD_MB + kept)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    let state = ledger.lock();
    let cal = &state.calibration[&("g/twin".to_owned(), GPU.to_owned())];
    assert_eq!((cal.ram_startup_mb, cal.ram_first_units), (900, 2_000_000));
}

/// A pixel-priced replica's single-item window holds one image per batch:
/// its unit budget is the image's pixels, not one pixel.
#[test]
fn a_pixel_priced_single_item_window_is_one_image() {
    const IMAGE: u64 = 1_048_576;
    let cost = CostDimension {
        unit: CostUnit::Pixel,
        aggregation: Some(CostAggregation::Sum),
        epoch: 1,
        seed_units: Some(2_000_000),
        degraded: false,
        canvas_pixels: Some(IMAGE as u32),
        max_tokens: None,
    };
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/pixels", GPU, cost);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let grant = single_item_window(&handle, &admission, IMAGE, 50);
    assert_eq!(grant.unit_budget, IMAGE);
    assert!(grant.mb > 0, "the GPU side reserves for the image");

    // Two such images fill the seed: after one that grew no RAM the cap
    // would hold a seed batch, and ends.
    let (handle, admission) = cold_gpu_replica(&ledger, "g/pixels-flat", GPU, cost);
    let token = admission.request_grant(IMAGE, None, 1, 0).expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            items: Some(1),
            ..ram_batch(IMAGE, RSS_AT_LOAD_MB, RSS_AT_LOAD_MB)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(item_bound(&admission), usize::MAX);
}

/// A cold replica with a RAM side, alone on a card with `room_mb` for its
/// batches: wd-vit on an AMD card unless changed (base 554 MiB, seed 64,
/// 82 MiB per unit).
struct ColdRun {
    room_mb: u64,
    seed: u32,
    /// GPU MiB a batch allocates per unit; the pool is kept.
    per_unit_mb: u64,
    /// GPU MiB only the replica's first batch allocates.
    first_only_mb: u64,
    profile: Option<ProfileSeed>,
}

impl ColdRun {
    fn beside(room_mb: u64) -> Self {
        Self {
            room_mb,
            seed: 64,
            per_unit_mb: 82,
            first_only_mb: 0,
            profile: None,
        }
    }

    /// `windows` windows under a full queue, at `rate(units)` units/s. Host
    /// RAM is plentiful and handed back after each batch. The single-item
    /// window holds one batch, the others [`WINDOW_DEPTH_MULTIPLIER`]. A
    /// batch that needs more than the room is out of memory: it panics.
    fn run(self, windows: usize, rate: impl Fn(u64) -> f64) -> Ran {
        const BASE_MB: u64 = 554;
        let profiles = Arc::new(FakeProfiles {
            seed: self.profile,
            ..FakeProfiles::default()
        });
        let ledger = host_with_cards(
            &[GPU],
            BASE_MB + self.room_mb,
            Some(Arc::clone(&profiles)),
            CPU_RAM_MB,
        );
        let handle = with_rss(loaded_on(GPU, Some(BASE_MB), Some(0)));
        let admission = ledger
            .register_worker("g/cold", item_cost(self.seed), &handle, Some(GPU))
            .expect("admitted on its card");
        cpu_free_to_book(&ledger, 45_000);
        let mut pool = 0;
        let mut first_only = self.first_only_mb;
        let mut ran = Vec::new();
        for window in 0..windows {
            ledger.record_free_for_test(GPU, self.room_mb - pool);
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let grant = *token.grant();
            assert!(
                !row(&ledger, "g/cold").ram_ceiling_binding,
                "window {window}: host RAM is plentiful"
            );
            let units = grant
                .unit_budget
                .min(grant.user_cap_items.map_or(u64::MAX, u64::from));
            let batches = match grant.user_cap_items {
                Some(1) => 1,
                _ => WINDOW_DEPTH_MULTIPLIER,
            };
            let measurements = (0..batches)
                .map(|_| {
                    let need = self.per_unit_mb * units + std::mem::take(&mut first_only);
                    assert!(
                        need <= self.room_mb,
                        "window {window}: {units} units need {need} MiB, out of memory: {ran:?}"
                    );
                    let before = pool;
                    pool = pool.max(need);
                    BatchMeasurement {
                        reserved_before_mb: Some(before),
                        reserved_after_mb: Some(pool),
                        allocated_before_mb: Some(0),
                        peak_allocated_mb: Some(need),
                        peak_rss_mb: Some(RSS_AT_LOAD_MB + RAM_PER_UNIT_MB * units),
                        rss_after_mb: Some(RSS_AT_LOAD_MB),
                        duration_ms: Some(units as f64 * 1000.0 / rate(units)),
                        ..measurement(units, 0, pool)
                    }
                })
                .collect();
            handle.lock().unwrap().record_measurements(measurements);
            token.finish(WindowOutcome::Responded { oom: None });
            ran.push(units);
        }
        let health = row(&ledger, "g/cold");
        assert_eq!(health.deflation, 0);
        let stored = profiles.updates.lock().unwrap().last().cloned();
        Ran {
            units: ran,
            knee: health.knee_units,
            stored: stored.map(|row| (row.max_units_measured, row.knee_units)),
        }
    }
}

/// What a [`ColdRun`] did: each window's units, the knee in force at the
/// end, and the last store row's `(largest size run here, knee)`.
struct Ran {
    units: Vec<u64>,
    knee: Option<u64>,
    stored: Option<(u64, Option<u64>)>,
}

/// Items per second of a model bound by CPU preprocessing: flat from 8 units.
const CPU_BOUND: [(u64, f64); 6] = [
    (1, 8.0),
    (2, 14.0),
    (4, 21.0),
    (8, 22.5),
    (16, 22.3),
    (512, 21.0),
];

/// A cold load of a model whose rate stops rising at 8 units, on an idle
/// 16 GB card. The item cap and the ratchet take it from the single item to
/// the seed, 64 units, where it opens. 128 units measure no faster, the 256
/// past them do not fit and are not run, and the working size steps down to
/// 8, the smallest within 5 % of the best rate, on what the way up
/// measured. Later trials run 16, 32 and 4 and leave it there, far from the
/// 191 units the card holds. The store gets the largest size it ran and the
/// working size.
#[test]
fn a_cold_load_opens_at_the_seed_and_steps_down_to_where_the_rate_stops_rising() {
    let ran = ColdRun::beside(15_700).run(50, |units| ladder_rate(&CPU_BOUND, units));
    assert_eq!(
        ran.units[..11],
        [1, 2, 4, 8, 16, 32, 64, 64, 128, 64, 8],
        "{:?}",
        ran.units
    );
    let trials: Vec<usize> = (10..ran.units.len())
        .filter(|window| ran.units[*window] != 8)
        .collect();
    assert_eq!(trials, [22, 23, 24, 37, 38, 39], "{:?}", ran.units);
    assert_eq!(ran.units[22..25], [16, 32, 4]);
    assert_eq!(ran.knee, Some(8));
    assert_eq!(ran.stored, Some((128, Some(8))));
}

/// The same load with a rate that rises 1.41x per doubling grows to what
/// the card holds, and no batch runs out of memory at the fitted price. The
/// size is stored as it is reached; the next trial, a window at half of it,
/// leaves it in place.
#[test]
fn a_cold_load_whose_rate_keeps_rising_grows_to_the_room() {
    let ran = ColdRun::beside(15_700).run(26, |units| (units as f64).sqrt());
    assert_eq!(ran.units[..10], [1, 2, 4, 8, 16, 32, 64, 64, 128, 191]);
    assert_eq!(ran.units[23], 95, "{:?}", ran.units);
    assert!(
        (10..26)
            .filter(|window| *window != 23)
            .all(|window| ran.units[window] == 191)
    );
    assert_eq!(ran.stored, Some((191, Some(191))));
}

/// Beside another process that holds most of the card, no batch is larger
/// than the room covers: 26 units in 2 186 MiB where the seed's 64 need
/// 5 248, and in 295 MiB three units, cut at the price the first two sizes
/// measured, where four need 328.
#[test]
fn a_cold_load_beside_a_full_card_stays_inside_its_room() {
    let rising = |units| ladder_rate(&RISING, units);
    let ran = ColdRun::beside(2_186).run(8, rising);
    assert_eq!(ran.units, [1, 2, 4, 8, 16, 26, 26, 26]);
    let ran = ColdRun::beside(295).run(6, rising);
    assert_eq!(ran.units, [1, 2, 3, 3, 3, 3]);
}

/// One measured size prices a further unit at the registry's design figure
/// (256 MiB at a seed of 8), which is no ground to cut a batch on: a model
/// of 5 MiB per unit in a 300 MiB room grows to the 60 units that fit, not
/// held at one. A store row holding that one sample does not hold the next
/// run either.
#[test]
fn one_measured_size_does_not_hold_a_small_room_at_one_unit() {
    let rising = |units| ladder_rate(&RISING, units);
    let small = |profile| ColdRun {
        seed: 8,
        per_unit_mb: 5,
        profile,
        ..ColdRun::beside(300)
    };
    let ran = small(None).run(9, rising);
    assert_eq!(ran.units, [1, 2, 4, 8, 8, 16, 32, 60, 60]);
    let one_sample = ProfileSeed {
        slope_mb_per_unit: 0.0,
        samples: 1,
        max_units_measured: 1,
        local_samples: 1,
        ring: vec![FitSample {
            units: 1,
            delta_mb: 5,
        }],
        ..seeded_anchor(1, true)
    };
    let ran = small(Some(one_sample)).run(9, rising);
    assert_eq!(ran.units, [1, 2, 4, 8, 8, 16, 32, 60, 60]);
}

/// A run resumed from a store row that an earlier build wrote without a
/// working size (largest size 179, on the same card) opens at the seed once
/// the item cap ends, not at the anchor or the room. 128 units measure no
/// faster than 64, the 256 past them do not fit and are not run, and the
/// size steps down to 8 units, the smallest within 5 % of the best rate;
/// the row is stored with that.
#[test]
fn a_stored_anchor_without_a_working_size_opens_at_the_seed() {
    let stored = ProfileSeed {
        base_mb: 554,
        slope_mb_per_unit: 81.7,
        knee_units: None,
        ..seeded_anchor(179, true)
    };
    let resumed = ColdRun {
        profile: Some(stored),
        ..ColdRun::beside(15_700)
    };
    let ran = resumed.run(22, |units| ladder_rate(&CPU_BOUND, units));
    assert_eq!(ran.units[..10], [1, 2, 4, 64, 64, 128, 64, 32, 16, 8]);
    assert!(ran.units[10..].iter().all(|units| *units == 8));
    assert_eq!(ran.stored, Some((128, Some(8))));
}

/// A replica whose batches never grow host RAM still learns its GPU side:
/// its item-capped windows are fit samples, three sizes of them a fit. The
/// cap ends once filled doublings would hold a seed batch; from there its
/// batches set the anchor as any replica's do. Nothing is booked.
#[test]
fn a_replica_that_never_grows_host_ram_still_learns_its_gpu_side() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/flat", GPU, item_cost(8));
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let mut bounds = Vec::new();
    for window_units in [64, 64, 64, 64, 4, 2] {
        bounds.push(item_bound(&admission));
        let token = admission
            .request_grant(window_units, None, 1, 0)
            .expect("granted");
        let grant = *token.grant();
        let ran = grant
            .unit_budget
            .min(grant.user_cap_items.map_or(u64::MAX, u64::from));
        handle.lock().unwrap().record_measurements(vec![ram_batch(
            ran,
            RSS_AT_LOAD_MB,
            RSS_AT_LOAD_MB,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });
        admission.earn_next_size();
    }
    assert_eq!(bounds[..4], [1, 2, 4, usize::MAX]);
    let state = ledger.calibration_state("g/flat", GPU).expect("calibrated");
    assert_eq!(state.max_units_measured, 8, "the seed batch it ran");
    assert_eq!(state.samples.len(), 4, "sizes 1, 2, 4 and 8");
    assert!(state.fit.is_some());
    let flat = row(&ledger, "g/flat");
    assert_eq!((flat.ram_mb_per_unit, flat.ram_booked_mb), (None, 0));

    // Sent one item at a time the cap never fills: each window is a
    // one-unit window to the GPU side.
    let (handle, admission) = cold_gpu_replica(&ledger, "g/single", GPU, item_cost(8));
    let mut bounds = Vec::new();
    for _ in 0..4 {
        bounds.push(item_bound(&admission));
        let token = admission.request_grant(1, None, 1, 0).expect("granted");
        handle.lock().unwrap().record_measurements(vec![ram_batch(
            1,
            RSS_AT_LOAD_MB,
            RSS_AT_LOAD_MB,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    assert_eq!(bounds, [1, 2, 2, 2]);
    let single = ledger
        .calibration_state("g/single", GPU)
        .expect("calibrated");
    assert_eq!((single.max_units_measured, single.samples.len()), (1, 1));
}

/// The RAM cost belongs to the (model, GPU) and outlives the replica: a
/// reload with it known books from its first window; one without it runs a
/// single-item window again.
#[test]
fn a_reload_runs_a_single_item_window_only_while_the_cost_is_unknown() {
    let ledger = host(&[GPU], None);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/reload", GPU, item_cost(8));
    measure_ram_cost(&handle, &admission, 0, RAM_PER_UNIT_MB);
    drop(admission);
    let (_handle, admission) = cold_gpu_replica(&ledger, "g/reload", GPU, item_cost(8));
    assert_eq!(item_bound(&admission), usize::MAX);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    // The seed, with no item cap: no capped window is a working size.
    assert_eq!(
        (token.grant().unit_budget, token.grant().user_cap_items),
        (8, None)
    );
    assert_eq!(row(&ledger, "g/reload").ram_booked_mb, 8 * RAM_PER_UNIT_MB);
    drop(token);
    drop(admission);

    let (handle, admission) = cold_gpu_replica(&ledger, "g/unknown", GPU, item_cost(8));
    single_item_window(&handle, &admission, 1, 0);
    assert_eq!(item_bound(&admission), 2);
    drop(admission);
    let (_handle, admission) = cold_gpu_replica(&ledger, "g/unknown", GPU, item_cost(8));
    assert_eq!(item_bound(&admission), 1);
}

/// An out-of-memory in the single-item window, from a batch or from the
/// error frame alone, deflates as in any window; after a failed batch the
/// next window is again a single item.
#[test]
fn a_single_item_window_out_of_memory_deflates() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = cold_gpu_replica(&ledger, "g/oom", GPU, item_cost(8));
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    let token = admission.request_grant(1, None, 1, 0).expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            oom_class: Some(OomClass {
                source: OOM_SOURCE_TYPED.to_owned(),
                exception: "torch.OutOfMemoryError".to_owned(),
                free_mb_at_failure: Some(0),
                device: "cuda:0".to_owned(),
            }),
            ..ram_batch(1, RSS_AT_LOAD_MB + RAM_PER_UNIT_MB, RSS_AT_LOAD_MB)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(row(&ledger, "g/oom").deflation, 1);
    assert_eq!(item_bound(&admission), 1);

    // An out-of-memory the error frame reported, its batch clean.
    let token = admission.request_grant(1, None, 1, 0).expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![ram_batch(1, RSS_AT_LOAD_MB, RSS_AT_LOAD_MB)]);
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    assert_eq!(row(&ledger, "g/oom").deflation, 2);
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

/// An item-capped window of a fitted model is priced with the fixed part
/// like any other: one unit at the default pool margin, 1.25 × (100 + 10).
#[test]
fn an_item_capped_window_is_priced_with_the_fixed_part() {
    let ring = [4u64, 8, 16].map(|units| FitSample {
        units,
        delta_mb: 100 + 10 * units,
    });
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            ring: ring.to_vec(),
            ..seeded_anchor(16, true)
        }),
        ..FakeProfiles::default()
    });
    let ledger = host(&[GPU], Some(profiles));
    let (_handle, admission) = cold_gpu_replica(&ledger, "g/a", GPU, item_cost(4));
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let grant = *token.grant();
    assert_eq!(
        (grant.user_cap_items, grant.unit_budget, grant.mb),
        (Some(1), 1, 138)
    );
}

/// A window the GPU's room cut and host RAM then cut further was sized by
/// host RAM: its failure leaves the pool margin. With RAM to spare the room
/// sizes it, and its failure raises the margin.
#[test]
fn an_out_of_memory_window_host_ram_sized_leaves_the_pool_margin() {
    let ledger = host(&[GPU], None);
    let (handle, admission) = gpu_replica(&ledger, "g/a", GPU, 3);
    ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000);
    for _ in 0..7 {
        ram_window(&handle, &admission);
    }
    // The card has room for 100 units of the 384 asked: 100 + 100 × 10 MiB.
    push_memory(&handle, 100, 1000);
    ledger.ingest_all_for_test();
    for (ram_to_book, units, raised) in [(500, 50, 0), (45_000, 100, 1)] {
        cpu_free_to_book(&ledger, ram_to_book);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, units);
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Marker),
        });
        assert_eq!(margin_steps(&ledger, "g/a", GPU), raised);
    }
}
