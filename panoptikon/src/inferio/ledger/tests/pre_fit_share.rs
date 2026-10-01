//! Pre-fit reservations beside other replicas: at most an equal part of the
//! headroom, at least the batch's design cost, and a batch cut to what its
//! reservation covers while a neighbour holds one.
use super::*;
use crate::inferio::cost::SEED_BUDGET_MB;
use crate::inferio::gpu::GpuInventory;

/// A 16 GiB card with 15 333 MiB of headroom over two pre-fit models.
const CARD_MB: u64 = 16_368;
const CLIP_BASE_MB: u64 = 435;
const TAGS_BASE_MB: u64 = 600;
const HEADROOM_MB: u64 = CARD_MB - CLIP_BASE_MB - TAGS_BASE_MB;

/// What a seed batch is designed to grow the pool by: the seed budget times
/// the default pool margin.
const SEED_BATCH_MB: u64 = 2560;

fn pre_fit(ledger: &Arc<VramLedger>, model: &str, base_mb: u64, seed: u32) -> Admission {
    ledger
        .register_worker(
            model,
            item_cost(seed),
            &loaded(Some(base_mb), Some(0)),
            None,
        )
        .expect("registers")
}

fn window(admission: &Admission) -> GrantToken {
    admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted")
}

/// A cold replica whose batches cost what its seed was sized for.
struct Cold {
    handle: TelemetryHandle,
    admission: Admission,
    seed: u64,
    /// The pool stays with the process after a batch, as on a GPU; host RAM
    /// is handed back.
    keeps_pool: bool,
    pool_mb: u64,
    open: Option<GrantToken>,
}

impl Cold {
    fn new(handle: TelemetryHandle, admission: Admission, seed: u64, keeps_pool: bool) -> Self {
        Self {
            handle,
            admission,
            seed,
            keeps_pool,
            pool_mb: 0,
            open: None,
        }
    }

    fn on_gpu(ledger: &Arc<VramLedger>, model: &str, base_mb: u64, seed: u32) -> Self {
        let handle = loaded(Some(base_mb), Some(0));
        let admission = ledger
            .register_worker(model, item_cost(seed), &handle, None)
            .expect("registers");
        Self::new(handle, admission, u64::from(seed), true)
    }

    fn cost_mb(&self, units: u64) -> u64 {
        (units * SEED_BATCH_MB).div_ceil(self.seed)
    }

    /// The pool it holds, or what its open window is designed to grow it to.
    fn designed_mb(&self) -> u64 {
        let open = self.open.as_ref().map(|token| token.grant().unit_budget);
        self.pool_mb
            .max(open.map_or(0, |units| self.cost_mb(units)))
    }

    /// The open window ran one clean batch at its budget.
    fn settle(&mut self) {
        let Some(token) = self.open.take() else {
            return;
        };
        let units = token.grant().unit_budget;
        let peak = self.cost_mb(units);
        self.pool_mb = if self.keeps_pool {
            self.pool_mb.max(peak)
        } else {
            0
        };
        self.handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                peak_allocated_mb: Some(units * SEED_BUDGET_MB / self.seed),
                reserved_after_mb: Some(self.pool_mb),
                ..measurement(units, 0, peak)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
}

/// Windows 1 to 3 of a cold start under a full queue: each replica in turn
/// settles its window and asks again while the others hold theirs. Returns
/// each window's `(mb, units)` per replica. After every grant, what the
/// replicas hold or are designed to use fits in `headroom`; a replica left
/// with less than one unit's cost still runs one unit, which `floor_mb`
/// allows for.
fn cold_start(replicas: &mut [Cold], headroom: u64, floor_mb: u64) -> Vec<Vec<(u64, u64)>> {
    (1..=3)
        .map(|round| {
            (0..replicas.len())
                .map(|index| {
                    replicas[index].settle();
                    let token = window(&replicas[index].admission);
                    let granted = (token.grant().mb, token.grant().unit_budget);
                    replicas[index].open = Some(token);
                    let designed: u64 = replicas.iter().map(Cold::designed_mb).sum();
                    assert!(
                        designed <= headroom + floor_mb,
                        "window {round}, replica {index}: {designed} > {headroom}"
                    );
                    granted
                })
                .collect()
        })
        .collect()
}

/// Two pre-fit models on one GPU: the first reserves half the headroom, the
/// second half of what is left, and both run their seed batch.
#[test]
fn two_pre_fit_models_each_reserve_a_part_and_run_their_seed_batch() {
    let ledger = ledger(CARD_MB, no_margin());
    let clip = pre_fit(&ledger, "g/clip", CLIP_BASE_MB, 8);
    let tags = pre_fit(&ledger, "g/tags", TAGS_BASE_MB, 4);
    ledger.record_free_for_test(GPU, HEADROOM_MB);
    assert_eq!(ledger.headroom_mb(GPU), 15_333);

    let first = window(&clip);
    assert_eq!(first.grant().mb, 7666, "15 333 / 2");
    assert_eq!(first.grant().unit_budget, 8);
    let second = window(&tags);
    assert_eq!(second.grant().mb, 3833, "(15 333 - 7666) / 2");
    assert_eq!(
        second.grant().unit_budget,
        4,
        "its seed batch, not one unit"
    );
    assert!(!second.grant().squeezed);
    assert!(ledger.take_pending_trims().is_empty());

    // Back-to-back windows: each asks while the other holds its part.
    drop(first);
    let first = window(&clip);
    assert_eq!(first.grant().mb, 5750, "(15 333 - 3833) / 2");
    drop(second);
    let second = window(&tags);
    assert_eq!(second.grant().mb, 4791, "(15 333 - 5750) / 2");
    assert_eq!(
        (first.grant().unit_budget, second.grant().unit_budget),
        (8, 4)
    );
}

/// A load in flight counts as a replica: a window granted while the second
/// model is still loading leaves it room for its first window.
#[tokio::test]
async fn a_window_granted_during_a_load_leaves_room_for_the_loading_model() {
    let ledger = ledger(CARD_MB, no_margin());
    let clip = pre_fit(&ledger, "g/clip", CLIP_BASE_MB, 8);
    ledger.record_free_for_test(GPU, CARD_MB - CLIP_BASE_MB);
    let loading = ledger
        .reserve_load_for_test("g/tags", item_cost(4), GPU, None)
        .await
        .expect("known GPU");
    let headroom = ledger.headroom_mb(GPU);
    assert_eq!(headroom, CARD_MB - CLIP_BASE_MB - CONSERVATIVE_BASE_MB);

    let first = window(&clip);
    assert_eq!(first.grant().mb, headroom / 2);
    drop(loading);
    let tags = pre_fit(&ledger, "g/tags", TAGS_BASE_MB, 4);
    let second = window(&tags);
    assert_eq!(second.grant().mb, (HEADROOM_MB - first.grant().mb) / 2);
    assert_eq!(second.grant().unit_budget, 4);
}

/// Three pre-fit replicas asking one after another: a third of what each
/// sees, or the seed batch's design cost when that is more, and headroom is
/// left over.
#[test]
fn three_pre_fit_replicas_each_reserve_a_third_of_what_they_see() {
    let ledger = ledger(18_000, no_margin());
    let replicas: Vec<Admission> = (0..3)
        .map(|index| pre_fit(&ledger, &format!("g/m{index}"), 1000, 4))
        .collect();
    ledger.record_free_for_test(GPU, 15_000);
    let tokens: Vec<GrantToken> = replicas.iter().map(window).collect();
    let reserved: Vec<u64> = tokens.iter().map(|token| token.grant().mb).collect();
    assert_eq!(reserved, vec![5000, 3333, SEED_BATCH_MB]);
    assert!(
        tokens
            .iter()
            .all(|token| token.grant().unit_budget == 4 && !token.grant().squeezed)
    );
    assert_eq!(ledger.headroom_mb(GPU), 15_000 - 10_893);
}

/// A pre-fit replica joining two fitted ones that are in a window reserves a
/// third of the headroom, so a fitted one can still grow its next window.
/// Only the pre-fit share is cut: the fitted one takes more than a third.
#[test]
fn a_pre_fit_replica_joining_busy_fitted_ones_leaves_them_room_to_grow() {
    let ledger = ledger(31_200, no_margin());
    // 400 MiB per unit, measured up to 24 units, which left a 9600 MiB pool.
    let fitted: Vec<(TelemetryHandle, Admission)> = (0..2)
        .map(|index| {
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker(&format!("g/fitted{index}"), item_cost(4), &handle, None)
                .expect("registers");
            let series = (1..=6u64).map(|k| measurement(k * 4, 0, 400 * k * 4));
            handle.lock().unwrap().record_measurements(series.collect());
            clean_window(&admission);
            (handle, admission)
        })
        .collect();
    let joining = pre_fit(&ledger, "g/joining", 1000, 4);
    // 3 bases and 2 pools are ours: 31 200 - 3000 - 19 200 is free.
    ledger.record_free_for_test(GPU, 9000);

    let busy: Vec<GrantToken> = fitted
        .iter()
        .map(|(_, admission)| admission.request_grant(24, None, 1, 0).expect("granted"))
        .collect();
    assert!(busy.iter().all(|token| token.grant().mb == 9600));
    assert_eq!(ledger.headroom_mb(GPU), 9000, "spent inside their pools");

    let token = window(&joining);
    assert_eq!(token.grant().mb, 3000, "9000 / 3");
    assert_eq!(token.grant().unit_budget, 4);
    assert!(!token.grant().squeezed);

    // The first fitted replica's next window is 8 units larger: its pool
    // and 3200 of the 6000 left, more than a third of it.
    let mut busy = busy.into_iter();
    drop(busy.next());
    let grown = window(&fitted[0].1);
    assert_eq!(grown.grant().unit_budget, 32);
    assert_eq!(grown.grant().mb, 12_800);
    assert!(!grown.grant().squeezed);
}

/// A fitted requester's share is not cut in the appetite split either: with
/// a hungry pre-fit neighbour it takes 7500 of 12 000, more than half.
#[test]
fn a_fitted_requester_is_not_held_to_an_equal_part_in_the_split() {
    let ledger = ledger(16_000, no_margin());
    let handle = loaded(Some(3000), Some(0));
    let fitted = ledger
        .register_worker("g/fitted", item_cost(4), &handle, None)
        .expect("registers");
    // 1500 MiB per unit, 1875 with the default pool margin.
    ledger.install_fit_for_test(
        "g/fitted",
        GPU,
        FitSnapshot {
            slope_mb_per_unit: 1500.0,
            intercept_mb: 0.0,
            residual_mb: 0.0,
            samples: 20,
            version: 1,
        },
    );
    let neighbour = pre_fit(&ledger, "g/neighbour", 1000, 4);
    ledger.record_free_for_test(GPU, 12_000);
    fitted.note_demand(1);
    neighbour.note_demand(1);

    let token = window(&fitted);
    assert_eq!(token.grant().unit_budget, 4);
    assert_eq!(token.grant().mb, 7500);
    assert!(!token.grant().squeezed);
}

/// Two of three replicas ask together: each share is cut to a third of the
/// headroom, not to half.
#[test]
fn the_split_is_cut_to_a_part_among_all_replicas_not_only_the_hungry() {
    let ledger = ledger(18_000, no_margin());
    let replicas: Vec<Admission> = (0..3)
        .map(|index| pre_fit(&ledger, &format!("g/m{index}"), 1000, 4))
        .collect();
    ledger.record_free_for_test(GPU, 15_000);
    replicas[0].note_demand(1);
    replicas[1].note_demand(1);
    assert_eq!(window(&replicas[0]).grant().mb, 5000, "not 15 000 / 2");
}

/// A stored fit with a non-positive slope prices nothing, so its replica is
/// pre-fit: beside a neighbour it reserves its part, not the whole headroom.
#[test]
fn a_replica_whose_fit_cannot_price_reserves_a_part() {
    let ledger = ledger(CARD_MB, no_margin());
    let flat = pre_fit(&ledger, "g/flat", CLIP_BASE_MB, 8);
    let _neighbour = pre_fit(&ledger, "g/tags", TAGS_BASE_MB, 4);
    ledger.install_fit_for_test(
        "g/flat",
        GPU,
        FitSnapshot {
            slope_mb_per_unit: 0.0,
            intercept_mb: 300.0,
            residual_mb: 0.0,
            samples: 20,
            version: 1,
        },
    );
    ledger.record_free_for_test(GPU, HEADROOM_MB);
    assert_eq!(window(&flat).grant().mb, 7666);
}

/// The two-model cold start through its three pre-fit windows: the ramp
/// doubles until the two batches' design cost would pass the headroom, then
/// each batch is cut to what its reservation covers.
#[test]
fn the_two_model_cold_start_stays_inside_the_headroom() {
    let ledger = ledger(CARD_MB, no_margin());
    let mut replicas = [
        Cold::on_gpu(&ledger, "g/clip", CLIP_BASE_MB, 8),
        Cold::on_gpu(&ledger, "g/tags", TAGS_BASE_MB, 64),
    ];
    ledger.record_free_for_test(GPU, HEADROOM_MB);
    let windows = cold_start(&mut replicas, HEADROOM_MB, 0);
    assert_eq!(windows[0], [(7666, 8), (3833, 64)]);
    assert_eq!(windows[1], [(7030, 16), (5431, 128)]);
    // 32 and 256 units would be designed to use 20 480 MiB.
    assert_eq!(windows[2], [(9902, 30), (5431, 135)]);
    assert!(replicas.iter().all(|cold| {
        let grant = cold.open.as_ref().expect("a window").grant();
        grant.squeezed
    }));
}

/// An 8 GiB card with two cold models: the first reserves its seed batch's
/// design cost, more than half the headroom, and the second is cut to the
/// three units the rest covers.
#[test]
fn on_an_8_gib_card_the_second_cold_model_is_cut_to_what_is_left() {
    let ledger = ledger(8192, no_margin());
    let mut replicas = [
        Cold::on_gpu(&ledger, "g/a", 2000, 8),
        Cold::on_gpu(&ledger, "g/b", 2375, 8),
    ];
    ledger.record_free_for_test(GPU, 3817);
    let windows = cold_start(&mut replicas, 3817, 0);
    assert_eq!(windows[0], [(SEED_BATCH_MB, 8), (1257, 3)]);
    // Nothing is left to double into: both stay at what they hold.
    assert_eq!(windows[1], windows[0]);
    assert_eq!(windows[2], windows[0]);
    assert!(replicas.iter().all(|cold| {
        let grant = cold.open.as_ref().expect("a window").grant();
        grant.squeezed
    }));
}

/// With less headroom than its seed batch is designed to cost and nobody in
/// a window, a cold model still takes all of it and runs the seed batch, as
/// it does alone; the neighbour's window is then unpriced, one unit.
#[test]
fn a_cold_model_on_a_nearly_full_card_runs_its_seed_batch_first() {
    let ledger = ledger(8192, no_margin());
    let large = loaded(Some(5000), Some(0));
    let asking = ledger
        .register_worker("g/large", item_cost(4), &large, None)
        .expect("registers");
    let neighbour = pre_fit(&ledger, "g/neighbour", 2000, 4);

    // 392 MiB of headroom and 800 MiB of the requester's own pool.
    push_memory(&large, 392, 800);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 392);
    let token = window(&asking);
    assert_eq!(token.grant().mb, 392 + 800);
    assert_eq!(token.grant().unit_budget, 4);

    let other = window(&neighbour);
    assert_eq!((other.grant().mb, other.grant().unit_budget), (0, 1));
}

/// A replica whose pool already covers its batch reserves the contention
/// floor on top, above half the headroom; the neighbour is cut to the one
/// unit the rest covers.
#[test]
fn a_replica_whose_pool_covers_its_batch_still_reserves_the_floor() {
    let ledger = ledger(10_392, no_margin());
    let large = loaded(Some(5000), Some(0));
    let asking = ledger
        .register_worker("g/large", item_cost(4), &large, None)
        .expect("registers");
    let neighbour = pre_fit(&ledger, "g/neighbour", 2000, 4);

    // 392 MiB of headroom and 3000 MiB of the requester's own pool.
    push_memory(&large, 392, 3000);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 392);
    let token = window(&asking);
    assert_eq!(token.grant().mb, SEED_BATCH_FLOOR_MB + 3000);
    assert_eq!(token.grant().unit_budget, 4);
    assert!(!token.grant().squeezed);

    let other = window(&neighbour);
    assert_eq!((other.grant().mb, other.grant().unit_budget), (136, 1));
    assert!(other.grant().squeezed);
}

/// A 16 GB host with only the CPU device (limit 12 000 MiB): two cold
/// replicas.
#[test]
fn two_cold_cpu_replicas_on_a_16_gb_host_stay_inside_the_headroom() {
    let (ledger, mut replicas) = cold_cpu_host(2, 1750);
    let headroom = ledger.headroom_mb(cpu::DEVICE_KEY);
    assert_eq!(headroom, 8500);
    let windows = cold_start(&mut replicas, headroom, 0);
    assert_eq!(windows[0], [(4250, 8), (SEED_BATCH_MB, 8)]);
    assert_eq!(windows[1], [(5120, 16), (3380, 10)]);
    assert_eq!(windows[2], windows[1]);
}

/// The same host with four cold replicas: the third is cut, and the fourth
/// has nothing left and runs one unit.
#[test]
fn four_cold_cpu_replicas_on_a_16_gb_host_are_cut_to_the_headroom() {
    let (ledger, mut replicas) = cold_cpu_host(4, 1375);
    let headroom = ledger.headroom_mb(cpu::DEVICE_KEY);
    assert_eq!(headroom, 6500);
    let one_unit = SEED_BATCH_MB / 8;
    let windows = cold_start(&mut replicas, headroom, one_unit);
    assert_eq!(
        windows[0],
        [(SEED_BATCH_MB, 8), (SEED_BATCH_MB, 8), (1380, 4), (0, 1)]
    );
    assert_eq!(windows[1], windows[0]);
    assert_eq!(windows[2], windows[0]);
}

/// `count` cold replicas of `base_mb` each on a 16 GB CPU-only host under the
/// shipped budget; nothing else holds RAM.
fn cold_cpu_host(count: u64, base_mb: u64) -> (Arc<VramLedger>, Vec<Cold>) {
    const RAM_MB: u64 = 16_000;
    let ledger = VramLedger::new(
        &GpuInventory::known_cpu(RAM_MB),
        VramBudget::default().into(),
        None,
    );
    let replicas = (0..count)
        .map(|index| {
            let handle = loaded_cpu(Some(RAM_MB));
            handle
                .lock()
                .unwrap()
                .load
                .as_mut()
                .expect("a load report")
                .value
                .base_mb = Some(base_mb);
            let admission = ledger
                .register_worker(&format!("g/c{index}"), item_cost(8), &handle, None)
                .expect("registers");
            Cold::new(handle, admission, 8, false)
        })
        .collect();
    ledger.record_free_for_test(cpu::DEVICE_KEY, RAM_MB - count * base_mb);
    (ledger, replicas)
}

/// A 16 GB Mac: a cold MPS replica and a cold CPU replica share its RAM, so
/// each one's window counts as the other's neighbour.
#[test]
fn a_cold_mps_and_cpu_replica_on_a_16_gb_mac_stay_inside_the_headroom() {
    const RAM_MB: u64 = 16_384;
    const RECOMMENDED_MAX_MB: u64 = RAM_MB / 4 * 3;
    const BASE_MB: u64 = 2500;
    let ledger = VramLedger::new(
        &GpuInventory::known_mps(RAM_MB),
        VramBudget::default().into(),
        None,
    );
    ledger.install_probe_stub(None);
    let mut replicas = Vec::new();
    for (model, device, handle) in [
        ("g/mps", MPS_GPU, loaded_mps(Some(RECOMMENDED_MAX_MB))),
        ("g/cpu", cpu::DEVICE_KEY, loaded_on_cpu(Some(RAM_MB))),
    ] {
        handle
            .lock()
            .unwrap()
            .load
            .as_mut()
            .expect("a load report")
            .value
            .base_mb = Some(BASE_MB);
        let admission = ledger
            .register_worker(model, item_cost(8), &handle, Some(device))
            .expect("admitted");
        replicas.push(Cold::new(handle, admission, 8, device == MPS_GPU));
    }
    // Nothing else holds RAM: both bases are ours.
    super::unified_memory::push_basis(
        &replicas[0].handle,
        RECOMMENDED_MAX_MB,
        RAM_MB,
        RAM_MB - 2 * BASE_MB,
        0,
        0,
    );
    ledger.ingest_all_for_test();
    ledger.record_free_for_test(cpu::DEVICE_KEY, RAM_MB - 2 * BASE_MB);
    let headroom = ledger.headroom_mb(MPS_GPU);
    assert_eq!(headroom, 7288);
    assert_eq!(ledger.headroom_mb(cpu::DEVICE_KEY), headroom);

    let windows = cold_start(&mut replicas, headroom, 0);
    assert_eq!(windows[0], [(3644, 8), (SEED_BATCH_MB, 8)]);
    // The MPS replica keeps its 2560 MiB pool, and the CPU replica's window
    // on the other device cuts its next batch from 16 units to 14.
    assert_eq!(windows[1], [(4728, 14), (SEED_BATCH_MB, 8)]);
    assert_eq!(windows[2], windows[1]);
}

/// The CPU device: two pre-fit CPU replicas share the RAM headroom the same
/// way.
#[test]
fn two_pre_fit_cpu_replicas_each_reserve_a_part_of_the_ram() {
    let ledger = cpu_ledger(no_margin());
    let replicas: Vec<Admission> = ["g/a", "g/b"]
        .iter()
        .map(|model| {
            ledger
                .register_worker(model, item_cost(4), &loaded_cpu(Some(CPU_RAM_MB)), None)
                .expect("registers")
        })
        .collect();
    // Nothing else holds RAM: both bases are ours.
    ledger.record_free_for_test(cpu::DEVICE_KEY, CPU_RAM_MB - 2000);
    let headroom = ledger.headroom_mb(cpu::DEVICE_KEY);

    let first = window(&replicas[0]);
    assert_eq!(first.grant().mb, headroom / 2);
    let second = window(&replicas[1]);
    assert_eq!(second.grant().mb, (headroom - headroom / 2) / 2);
    assert_eq!(second.grant().unit_budget, 4);
}
