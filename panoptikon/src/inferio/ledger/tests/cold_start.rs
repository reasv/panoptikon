//! Cold starts beside other replicas, window by window: what each replica is
//! granted, what its batches use, and when its cost is fitted.
use super::*;
use crate::inferio::cost::SEED_BUDGET_MB;
use crate::inferio::gpu::GpuInventory;

/// A cold replica with a cost of its own, which the ledger does not know.
struct Cold {
    model: String,
    device: String,
    handle: TelemetryHandle,
    admission: Admission,
    seed: u64,
    /// MiB a seed batch allocates.
    allocated_mb: u64,
    /// The pool a batch needs per MiB it allocates.
    pool_ratio: f64,
    /// The pool stays with the process after a batch, as on a GPU; host RAM
    /// is handed back.
    keeps_pool: bool,
    /// MiB its first batch allocates once and keeps.
    kept_mb: u64,
    /// MiB only its first batch allocates.
    first_only_mb: u64,
    /// The units its first window asks for.
    first_window: u64,
    batches: u64,
    pool_mb: u64,
    open: Option<GrantToken>,
}

impl Cold {
    /// A replica on [`GPU`] whose seed batch allocates `percent` of the seed
    /// budget, under the default pool margin.
    fn on_gpu(
        ledger: &Arc<VramLedger>,
        model: &str,
        base_mb: u64,
        seed: u32,
        percent: u64,
    ) -> Self {
        let handle = loaded(Some(base_mb), Some(0));
        let admission = ledger
            .register_worker(model, item_cost(seed), &handle, None)
            .expect("registers");
        Self {
            model: model.to_owned(),
            device: GPU.to_owned(),
            handle,
            admission,
            seed: u64::from(seed),
            allocated_mb: SEED_BUDGET_MB * percent / 100,
            pool_ratio: POOL_MARGIN_DEFAULT,
            keeps_pool: true,
            kept_mb: 0,
            first_only_mb: 0,
            first_window: u64::MAX,
            batches: 0,
            pool_mb: 0,
            open: None,
        }
    }

    /// MiB its next batch allocates at `units`.
    fn allocated_mb(&self, units: u64) -> u64 {
        let first_only = if self.batches == 0 {
            self.first_only_mb
        } else {
            0
        };
        self.kept_mb + (units * self.allocated_mb).div_ceil(self.seed) + first_only
    }

    /// The pool that batch needs.
    fn batch_mb(&self, units: u64) -> u64 {
        (self.allocated_mb(units) as f64 * self.pool_ratio).ceil() as u64
    }

    /// The pool it holds, or what its open window's batch will grow it to.
    fn in_use_mb(&self) -> u64 {
        let open = self.open.as_ref().map(|token| token.grant().unit_budget);
        self.pool_mb
            .max(open.map_or(0, |units| self.batch_mb(units)))
    }

    /// The open window ran clean batches at its budget.
    fn settle(&mut self) {
        let Some(token) = self.open.take() else {
            return;
        };
        let units = token.grant().unit_budget;
        let before = self.pool_mb;
        let peak = before.max(self.batch_mb(units));
        let allocated = self.allocated_mb(units);
        self.pool_mb = if self.keeps_pool { peak } else { 0 };
        self.batches += 1;
        // A full queue's window is several batches deep: the first may grow
        // the pool, the rest find it as that one left it. Those are untimed,
        // so the throughput ring stays out of these tables: every size
        // earns the next, as with a rate that rises.
        let batch = |before: u64| BatchMeasurement {
            reserved_before_mb: Some(before),
            reserved_after_mb: Some(self.pool_mb),
            allocated_before_mb: Some(0),
            peak_allocated_mb: Some(allocated),
            ..measurement(units, 0, peak)
        };
        let again = if self.keeps_pool {
            self.pool_mb
        } else {
            before
        };
        let mut batches = vec![batch(before)];
        batches.extend((1..WINDOW_DEPTH_MULTIPLIER).map(|_| BatchMeasurement {
            duration_ms: None,
            ..batch(again)
        }));
        self.handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        self.admission.earn_next_size();
    }

    /// Its cost is fitted with a slope a grant can be priced with.
    fn fitted(&self, ledger: &Arc<VramLedger>) -> bool {
        let fit = ledger
            .calibration_state(&self.model, &self.device)
            .and_then(|state| state.fit);
        fit.is_some_and(|fit| fit.slope_mb_per_unit > 0.0)
    }
}

/// What a run of back-to-back windows did.
#[derive(Debug, PartialEq)]
struct Run {
    /// Each window's unit budgets, one per replica.
    units: Vec<Vec<u64>>,
    /// The window each replica's cost was first fitted for.
    fitted: Vec<Option<usize>>,
    /// Per window, the most the replicas used past the headroom; negative
    /// is room left.
    over: Vec<i64>,
}

/// `windows` rounds under a full queue: each replica in turn settles its
/// window and asks again while the others hold theirs.
fn run(ledger: &Arc<VramLedger>, replicas: &mut [Cold], headroom: u64, windows: usize) -> Run {
    let mut fitted = vec![None; replicas.len()];
    let mut over = Vec::new();
    let units = (1..=windows)
        .map(|window| {
            let mut worst = i64::MIN;
            let budgets = (0..replicas.len())
                .map(|index| {
                    replicas[index].settle();
                    if fitted[index].is_none() && replicas[index].fitted(ledger) {
                        fitted[index] = Some(window);
                    }
                    let asked = match window {
                        1 => replicas[index].first_window,
                        _ => u64::MAX,
                    };
                    let token = replicas[index]
                        .admission
                        .request_grant(asked, None, 1, 0)
                        .expect("granted");
                    let budget = token.grant().unit_budget;
                    replicas[index].open = Some(token);
                    let used: u64 = replicas.iter().map(Cold::in_use_mb).sum();
                    worst = worst.max(used as i64 - headroom as i64);
                    budget
                })
                .collect();
            over.push(worst);
            budgets
        })
        .collect();
    Run {
        units,
        fitted,
        over,
    }
}

/// Two cold models with seeds 8 on a card with `headroom` MiB over their
/// 2000 MiB bases.
fn gpu_pair(headroom: u64, seeds: [u32; 2], percent: u64) -> (Arc<VramLedger>, Vec<Cold>) {
    let ledger = ledger(headroom + 4000, no_margin());
    let replicas = vec![
        Cold::on_gpu(&ledger, "g/a", 2000, seeds[0], percent),
        Cold::on_gpu(&ledger, "g/b", 2000, seeds[1], percent),
    ];
    ledger.record_free_for_test(GPU, headroom);
    (ledger, replicas)
}

/// `count` cold replicas on a 16 GB CPU-only host under the shipped budget
/// (limit 13 952 MiB, RAM less its reserve), `headroom` MiB over their bases.
/// A seed batch grows the resident set by `percent` of its design cost and
/// hands it back.
fn cpu_host(count: u64, headroom: u64, percent: u64) -> (Arc<VramLedger>, Vec<Cold>) {
    const RAM_MB: u64 = 16_000;
    let base_mb = (RAM_MB - cpu::ram_reserve_mb(RAM_MB) - headroom) / count;
    let ledger = unprobed(VramLedger::new(
        &GpuInventory::known_cpu(RAM_MB),
        VramBudget::default().into(),
        None,
    ));
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
            let model = format!("g/c{index}");
            let admission = ledger
                .register_worker(&model, item_cost(8), &handle, None)
                .expect("registers");
            Cold {
                model,
                device: cpu::DEVICE_KEY.to_owned(),
                handle,
                admission,
                seed: 8,
                allocated_mb: 2560 * percent / 100,
                pool_ratio: 1.0,
                keeps_pool: false,
                kept_mb: 0,
                first_only_mb: 0,
                first_window: u64::MAX,
                batches: 0,
                pool_mb: 0,
                open: None,
            }
        })
        .collect();
    ledger.record_free_for_test(cpu::DEVICE_KEY, RAM_MB - count * base_mb);
    (ledger, replicas)
}

/// A 16 GB Mac (recommended max 12 288 MiB) with a cold MPS replica whose
/// pool is `pool_ratio` times its tensors and a cold CPU replica, 2500 MiB
/// of base each: 7288 MiB of headroom on both devices.
fn mac(pool_ratio: f64) -> (Arc<VramLedger>, Vec<Cold>) {
    const RAM_MB: u64 = 16_384;
    const RECOMMENDED_MAX_MB: u64 = RAM_MB / 4 * 3;
    const BASE_MB: u64 = 2500;
    // The CPU device capped at Metal's three quarters: the same room on both.
    let budgets = VramBudgets::default().with_gpu(
        cpu::DEVICE_KEY,
        VramBudget {
            cap_fraction: Some(0.75),
            ..VramBudget::default()
        },
    );
    let ledger = VramLedger::new(&GpuInventory::known_mps(RAM_MB), budgets, None);
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
        let on_mps = device == MPS_GPU;
        replicas.push(Cold {
            model: model.to_owned(),
            device: device.to_owned(),
            handle,
            admission,
            seed: 8,
            allocated_mb: if on_mps { SEED_BUDGET_MB } else { 2560 },
            pool_ratio: if on_mps { pool_ratio } else { 1.0 },
            keeps_pool: on_mps,
            kept_mb: 0,
            first_only_mb: 0,
            first_window: u64::MAX,
            batches: 0,
            pool_mb: 0,
            open: None,
        });
    }
    // Nothing else holds RAM: both bases are ours. The MPS reading carries
    // its RAM domain, as every Metal reading does.
    VramLedger::record_free_locked(
        &mut ledger.lock(),
        MPS_GPU,
        RECOMMENDED_MAX_MB - 2 * BASE_MB,
        "mps".to_owned(),
        std::time::Instant::now(),
        None,
        None,
        Some(RamBasis {
            total_mb: RAM_MB,
            available_mb: RAM_MB - 2 * BASE_MB,
        }),
    );
    ledger.record_free_for_test(cpu::DEVICE_KEY, RAM_MB - 2 * BASE_MB);
    assert_eq!(ledger.headroom_mb(MPS_GPU), 7288);
    assert_eq!(ledger.headroom_mb(cpu::DEVICE_KEY), 7288);
    (ledger, replicas)
}

/// An 8 GiB card, 3817 MiB of headroom, at a tenth, a half and all of the
/// design cost. The second window's increase is priced at the design cost
/// (15 units, not 16, at a tenth); from the third on at the rise measured.
/// A pair that fills the card is cut to the same sizes in every window, and
/// runs a unit or two fewer for two windows so that both are fitted at the
/// fourth.
#[test]
fn an_8_gib_pair_ramps_as_far_as_its_measured_cost_allows() {
    // (percent of the design cost, units, over)
    type Case = (u64, [[u64; 2]; 5], [i64; 5]);
    let cases: [Case; 3] = [
        (
            10,
            [[8, 3], [15, 6], [30, 12], [60, 24], [95, 24]],
            [-3465, -3146, -2477, -1139, -23],
        ),
        (
            50,
            [[8, 3], [12, 5], [16, 7], [16, 7], [16, 7]],
            [-2057, -1097, -137, -137, -137],
        ),
        (
            100,
            [[8, 3], [7, 2], [6, 1], [8, 3], [8, 3]],
            [-297, -297, -297, -297, -297],
        ),
    ];
    for (percent, units, over) in cases {
        let (ledger, mut replicas) = gpu_pair(3817, [8, 8], percent);
        let ran = run(&ledger, &mut replicas, 3817, 5);
        assert_eq!(ran.units, units, "{percent} %");
        assert_eq!(ran.fitted, [Some(4), Some(4)], "{percent} %");
        assert_eq!(ran.over, over, "{percent} %");
    }
}

/// A 12 GiB card, 7900 MiB of headroom, at the design cost and at half.
#[test]
fn a_12_gib_pair_stays_inside_the_headroom() {
    let (ledger, mut replicas) = gpu_pair(7900, [8, 8], 100);
    let ran = run(&ledger, &mut replicas, 7900, 5);
    assert_eq!(ran.units, [[8, 8], [16, 7], [15, 6], [16, 8], [16, 8]]);
    assert_eq!(ran.fitted, [Some(4), Some(4)]);
    assert_eq!(ran.over, [-2780, -220, -220, -220, -220]);

    let (ledger, mut replicas) = gpu_pair(7900, [8, 8], 50);
    let ran = run(&ledger, &mut replicas, 7900, 5);
    assert_eq!(ran.units, [[8, 8], [16, 16], [25, 24], [25, 24], [25, 24]]);
    assert_eq!(ran.fitted, [Some(4), Some(4)]);
    assert_eq!(ran.over, [-5340, -2780, -60, -60, -60]);
}

/// The 16 GiB card with 15 333 MiB of headroom and seeds 8 and 64: at the
/// design cost the third window is cut from 32 and 256 units; at a tenth of
/// it nothing is cut. Both fit at window 4 either way.
#[test]
fn the_16_gib_pair_is_cut_only_where_its_cost_passes_the_headroom() {
    let (ledger, mut replicas) = gpu_pair(15_333, [8, 64], 100);
    let ran = run(&ledger, &mut replicas, 15_333, 5);
    assert_eq!(
        ran.units,
        [[8, 64], [16, 128], [30, 135], [30, 143], [30, 143]]
    );
    assert_eq!(ran.fitted, [Some(4), Some(4)]);
    assert_eq!(ran.over, [-10_213, -5093, -333, -13, -13]);

    let (ledger, mut replicas) = gpu_pair(15_333, [8, 64], 10);
    let ran = run(&ledger, &mut replicas, 15_333, 5);
    assert_eq!(
        ran.units,
        [[8, 64], [16, 128], [32, 256], [64, 512], [128, 1024]]
    );
    assert_eq!(ran.fitted, [Some(4), Some(4)]);
}

/// A 16 GB host with only the CPU device. Host RAM is priced at 1.25 times
/// the resident growth a batch measured, so at the design cost the pair
/// settles a fifth under the headroom.
#[test]
fn cold_cpu_replicas_on_a_16_gb_host_stay_inside_the_headroom() {
    let (ledger, mut replicas) = cpu_host(2, 8500, 100);
    let ran = run(&ledger, &mut replicas, 8500, 5);
    assert_eq!(ran.units, [[8, 8], [16, 6], [14, 5], [14, 7], [14, 7]]);
    assert_eq!(ran.fitted, [Some(4), Some(4)]);
    assert_eq!(ran.over, [-3380, -820, -2100, -1780, -1780]);

    let (ledger, mut replicas) = cpu_host(2, 8500, 10);
    let ran = run(&ledger, &mut replicas, 8500, 5);
    assert_eq!(ran.units, [[8, 8], [16, 16], [32, 32], [64, 64], [128, 84]]);
    assert_eq!(ran.fitted, [Some(4), Some(4)]);

    // Four replicas: the last has nothing left in the first window and runs
    // one unit, 220 MiB past the headroom.
    let (ledger, mut replicas) = cpu_host(4, 6500, 100);
    let ran = run(&ledger, &mut replicas, 6500, 5);
    assert_eq!(
        ran.units,
        [
            [8, 8, 4, 1],
            [6, 6, 3, 1],
            [5, 5, 2, 1],
            [6, 6, 4, 1],
            [6, 6, 4, 1]
        ]
    );
    assert_eq!(ran.fitted, [Some(4), Some(4), Some(4), None]);
    assert_eq!(ran.over, [220, -420, -1700, -1060, -1060]);
}

/// A 16 GB Mac. The first MPS batch is priced at the default pool margin;
/// from the second on at the margin it measured. A pool 2.9 times its
/// tensors is 1212 MiB over in the first two windows and inside after.
#[test]
fn a_cold_mps_and_cpu_replica_on_a_16_gb_mac_are_priced_at_the_measured_pool() {
    let cases: [(f64, [[u64; 2]; 4], [i64; 4]); 3] = [
        (
            1.25,
            [[8, 8], [14, 6], [13, 5], [14, 7]],
            [-2168, -248, -888, -568],
        ),
        (
            2.3,
            [[8, 8], [7, 6], [6, 5], [8, 6]],
            [-17, -17, -657, -657],
        ),
        (
            2.9,
            [[8, 8], [7, 2], [6, 3], [8, 3]],
            [1212, 1212, -388, -388],
        ),
    ];
    for (pool_ratio, units, over) in cases {
        let (ledger, mut replicas) = mac(pool_ratio);
        let ran = run(&ledger, &mut replicas, 7288, 4);
        assert_eq!(ran.units, units, "pool ratio {pool_ratio}");
        assert_eq!(ran.over, over, "pool ratio {pool_ratio}");
    }
}

/// A pre-fit replica joining two fitted ones that hold their windows, with
/// 1800 MiB left: cut to 2 units at its design cost, then priced at what
/// that batch measured. Below the design cost it ramps and fits at window
/// 4; at the design cost only 1 and 2 units fit, which is no fit.
#[test]
fn a_joiner_beside_busy_fitted_replicas_fits_once_its_cost_is_measured() {
    let cases: [(u64, [u64; 5], Option<usize>); 3] = [
        (10, [2, 4, 8, 16, 28], Some(4)),
        (50, [2, 3, 5, 5, 5], Some(4)),
        (100, [2, 1, 2, 2, 2], None),
    ];
    for (percent, units, fitted) in cases {
        let ledger = ledger(9600, no_margin());
        // 100 MiB per unit, measured up to 24 units: a 2400 MiB pool each.
        let neighbours: Vec<(TelemetryHandle, Admission)> = (0..2)
            .map(|index| {
                let handle = loaded(Some(1000), Some(0));
                let admission = ledger
                    .register_worker(&format!("g/fitted{index}"), item_cost(4), &handle, None)
                    .expect("registers");
                let series = (1..=6u64).map(|k| measurement(k * 4, 0, 100 * k * 4));
                handle.lock().unwrap().record_measurements(series.collect());
                clean_window(&admission);
                (handle, admission)
            })
            .collect();
        let mut joiner = [Cold::on_gpu(&ledger, "g/joining", 1000, 4, percent)];
        ledger.record_free_for_test(GPU, 1800);
        let _busy: Vec<GrantToken> = neighbours
            .iter()
            .map(|(_, admission)| admission.request_grant(24, None, 1, 0).expect("granted"))
            .collect();
        assert_eq!(ledger.headroom_mb(GPU), 1800);

        let ran = run(&ledger, &mut joiner, 1800, 5);
        let budgets: Vec<u64> = ran.units.iter().map(|window| window[0]).collect();
        assert_eq!(budgets, units, "{percent} %");
        assert_eq!(ran.fitted, [fitted], "{percent} %");
        assert!(ran.over.iter().all(|over| *over < 0), "{percent} %");
    }
}

/// A replica whose first batch takes memory once, beside a cold neighbour
/// at the design cost that asks first and stays busy. What the first batch
/// kept, or needed only then, is not charged again for every unit: the
/// replica ramps or, where nothing is left to grow into, runs a unit fewer
/// for two windows, and is fitted. Once fitted, what it kept is priced once
/// per batch, so its windows take nothing more past the headroom. The
/// exception is a replica that has measured one unit only and has no room
/// for a second.
#[test]
fn memory_a_first_batch_takes_once_is_not_priced_per_unit() {
    // (headroom, kept MiB, first batch only MiB, MiB per seed batch, first
    // window, its units per window, the window it is fitted for)
    type Case = (u64, u64, u64, u64, u64, [u64; 6], Option<usize>);
    let cases: [Case; 6] = [
        // Known limit: one unit is all it has measured, the neighbour
        // leaves no room for a second, and none runs outside a reservation.
        (20_000, 8900, 0, 720, 1, [1; 6], None),
        (
            20_000,
            8900,
            0,
            720,
            u64::MAX,
            [8, 7, 6, 10, 10, 10],
            Some(4),
        ),
        (
            7900,
            0,
            4000,
            200,
            u64::MAX,
            [8, 7, 16, 32, 64, 88],
            Some(4),
        ),
        (7900, 4000, 0, 200, u64::MAX, [8, 7, 6, 1, 1, 1], Some(4)),
        (7900, 1000, 0, 800, 1, [1, 2, 4, 4, 4, 4], Some(4)),
        (7900, 0, 4000, 200, 1, [1, 1, 2, 4, 8, 16], Some(5)),
    ];
    for (headroom, kept_mb, first_only_mb, allocated_mb, first_window, units, fitted) in cases {
        let ledger = ledger(headroom + 4000, no_margin());
        let neighbour = Cold::on_gpu(&ledger, "g/b", 2000, 8, 100);
        let cold = Cold {
            kept_mb,
            first_only_mb,
            allocated_mb,
            first_window,
            ..Cold::on_gpu(&ledger, "g/a", 2000, 8, 100)
        };
        ledger.record_free_for_test(GPU, headroom);
        let ran = run(&ledger, &mut [neighbour, cold], headroom, 6);
        let budgets: Vec<u64> = ran.units.iter().map(|window| window[1]).collect();
        let case = (kept_mb, first_only_mb, first_window);
        assert_eq!(
            (budgets, ran.fitted[1]),
            (units.to_vec(), fitted),
            "{case:?}"
        );
        let pre_fit = ran.over[2].max(0);
        assert!(
            ran.over[3..].iter().all(|over| *over <= pre_fit),
            "{case:?}"
        );
    }
}
