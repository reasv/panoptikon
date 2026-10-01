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
            pool_mb: 0,
            open: None,
        }
    }

    /// The pool a batch of `units` needs.
    fn batch_mb(&self, units: u64) -> u64 {
        ((units * self.allocated_mb) as f64 * self.pool_ratio / self.seed as f64).ceil() as u64
    }

    /// The pool it holds, or what its open window's batch will grow it to.
    fn in_use_mb(&self) -> u64 {
        let open = self.open.as_ref().map(|token| token.grant().unit_budget);
        self.pool_mb
            .max(open.map_or(0, |units| self.batch_mb(units)))
    }

    /// The open window ran one clean batch at its budget.
    fn settle(&mut self) {
        let Some(token) = self.open.take() else {
            return;
        };
        let units = token.grant().unit_budget;
        let before = self.pool_mb;
        let peak = before.max(self.batch_mb(units));
        self.pool_mb = if self.keeps_pool { peak } else { 0 };
        self.handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                reserved_before_mb: Some(before),
                reserved_after_mb: Some(self.pool_mb),
                allocated_before_mb: Some(0),
                peak_allocated_mb: Some(units * self.allocated_mb / self.seed),
                ..measurement(units, 0, peak)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
    }

    fn fitted(&self, ledger: &Arc<VramLedger>) -> bool {
        let state = ledger.calibration_state(&self.model, &self.device);
        state.is_some_and(|state| state.fit.is_some())
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
                    let token = replicas[index]
                        .admission
                        .request_grant(u64::MAX, None, 1, 0)
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
/// (limit 12 000 MiB), `headroom` MiB over their bases. A seed batch grows
/// the resident set by `percent` of its design cost and hands it back.
fn cpu_host(count: u64, headroom: u64, percent: u64) -> (Arc<VramLedger>, Vec<Cold>) {
    const RAM_MB: u64 = 16_000;
    let base_mb = (12_000 - headroom) / count;
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
            pool_mb: 0,
            open: None,
        });
    }
    // Nothing else holds RAM: both bases are ours.
    ledger.record_free_for_test(MPS_GPU, RECOMMENDED_MAX_MB - 2 * BASE_MB);
    ledger.record_free_for_test(cpu::DEVICE_KEY, RAM_MB - 2 * BASE_MB);
    assert_eq!(ledger.headroom_mb(MPS_GPU), 7288);
    assert_eq!(ledger.headroom_mb(cpu::DEVICE_KEY), 7288);
    (ledger, replicas)
}

/// An 8 GiB card, 3817 MiB of headroom. A pair that costs a tenth of its
/// design ramps as its measurements come in and both fit at window 4; at
/// half, the first fits and the card is then full; at the design cost the
/// card is full from the first window, and both stay at what fits.
#[test]
fn an_8_gib_pair_ramps_as_far_as_its_measured_cost_allows() {
    // (percent of the design cost, units, fitted, over)
    type Case = (u64, [[u64; 2]; 5], [Option<usize>; 2], [i64; 5]);
    let cases: [Case; 3] = [
        (
            10,
            [[8, 3], [16, 6], [32, 12], [64, 24], [95, 24]],
            [Some(4), Some(4)],
            [-3466, -3115, -2414, -1012, -23],
        ),
        (
            50,
            [[8, 3], [16, 6], [17, 6], [17, 6], [17, 6]],
            [Some(4), None],
            [-2057, -297, -137, -137, -137],
        ),
        (
            100,
            [[8, 3], [8, 3], [8, 3], [8, 3], [8, 3]],
            [None, None],
            [-297, -297, -297, -297, -297],
        ),
    ];
    for (percent, units, fitted, over) in cases {
        let (ledger, mut replicas) = gpu_pair(3817, [8, 8], percent);
        let ran = run(&ledger, &mut replicas, 3817, 5);
        assert_eq!(ran.units, units, "{percent} %");
        assert_eq!(ran.fitted, fitted, "{percent} %");
        assert_eq!(ran.over, over, "{percent} %");
        if percent == 100 {
            let squeezed = |cold: &Cold| {
                cold.open
                    .as_ref()
                    .is_some_and(|token| token.grant().squeezed)
            };
            assert!(replicas.iter().all(squeezed), "a cut window is squeezed");
        }
    }
}

/// A 12 GiB card, 7900 MiB of headroom: at the design cost the pair stops at
/// 16 and 8 units with 220 MiB left; at half of it both fit at window 4.
#[test]
fn a_12_gib_pair_stays_inside_the_headroom() {
    let (ledger, mut replicas) = gpu_pair(7900, [8, 8], 100);
    let ran = run(&ledger, &mut replicas, 7900, 5);
    assert_eq!(ran.units, [[8, 8], [16, 8], [16, 8], [16, 8], [16, 8]]);
    assert_eq!(ran.fitted, [None, None]);
    assert_eq!(ran.over, [-2780, -220, -220, -220, -220]);

    let (ledger, mut replicas) = gpu_pair(7900, [8, 8], 50);
    let ran = run(&ledger, &mut replicas, 7900, 5);
    assert_eq!(ran.units, [[8, 8], [16, 16], [31, 18], [31, 18], [31, 18]]);
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
/// settles a quarter under the headroom.
#[test]
fn cold_cpu_replicas_on_a_16_gb_host_stay_inside_the_headroom() {
    let (ledger, mut replicas) = cpu_host(2, 8500, 100);
    let ran = run(&ledger, &mut replicas, 8500, 5);
    assert_eq!(ran.units, [[8, 8], [14, 6], [14, 6], [14, 6], [14, 6]]);
    assert_eq!(ran.fitted, [None, None]);
    assert_eq!(ran.over, [-3380, -1460, -2100, -2100, -2100]);

    let (ledger, mut replicas) = cpu_host(2, 8500, 10);
    let ran = run(&ledger, &mut replicas, 8500, 5);
    assert_eq!(ran.units, [[8, 8], [16, 16], [32, 32], [64, 64], [128, 84]]);
    assert_eq!(ran.fitted, [Some(4), Some(4)]);

    // Four replicas: the last has nothing left in the first window and runs
    // one unit, 220 MiB past the headroom.
    let (ledger, mut replicas) = cpu_host(4, 6500, 100);
    let ran = run(&ledger, &mut replicas, 6500, 5);
    assert_eq!(ran.units[0], [8, 8, 4, 1]);
    assert_eq!(ran.units[1..], [[6, 6, 3, 1]; 4]);
    assert_eq!(ran.over, [220, -420, -1380, -1380, -1380]);
}

/// A 16 GB Mac. The first MPS batch is priced at the default pool margin;
/// from the second on at the margin it measured. A pool 2.9 times its
/// tensors is 1212 MiB over in the first two windows and inside after.
#[test]
fn a_cold_mps_and_cpu_replica_on_a_16_gb_mac_are_priced_at_the_measured_pool() {
    let cases: [(f64, [[u64; 2]; 4], [i64; 4]); 3] = [
        (
            1.25,
            [[8, 8], [14, 6], [14, 6], [14, 6]],
            [-2168, -248, -888, -888],
        ),
        (
            2.3,
            [[8, 8], [8, 6], [8, 6], [8, 6]],
            [-17, -17, -657, -657],
        ),
        (
            2.9,
            [[8, 8], [6, 3], [8, 3], [8, 3]],
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
/// 4; at the design cost 2 units is what fits.
#[test]
fn a_joiner_beside_busy_fitted_replicas_fits_once_its_cost_is_measured() {
    let cases: [(u64, [u64; 5], Option<usize>); 3] = [
        (10, [2, 4, 8, 16, 28], Some(4)),
        (50, [2, 4, 5, 5, 5], Some(4)),
        (100, [2, 2, 2, 2, 2], None),
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
