//! Closed-loop batch-size simulator: the real ledger sizes every window, a
//! rate curve or a recorded trace says how long its batches take.
//!
//! `sizing_sim` is ignored by default. It reads one scenario per line from
//! `SIZING_SPEC` (`key=value` tokens, see [`Scenario`]), resolves trace names
//! against `SIZING_TRACES`, and writes one line per process start to
//! `SIZING_OUT`. `tools/calibration-protocol/sizing_table.py` builds the specs
//! and reads the output.
use super::*;
use crate::inferio::calibration::TrialCadence;
use crate::inferio::gpu::GpuInventory;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The worker's resident set at load, before any batch.
const RSS_AT_LOAD_MB: u64 = 2_000;

/// Units per second at a batch size: a synthetic curve.
#[derive(Clone, Debug)]
enum Curve {
    /// The same rate at every size.
    Flat(f64),
    /// `rate1 * gain^log2(units)`.
    Geo(f64, f64),
    /// [`Curve::Geo`] up to a size, flat above it.
    Knee(f64, u64, f64),
    /// Linear in log2(units) between `(units, rate)` points, flat outside.
    Ladder(Vec<(u64, f64)>),
    /// One curve before window `at`, another from it on.
    Switch(usize, Box<Curve>, Box<Curve>),
}

impl Curve {
    /// `flat:R`, `geo:G:R1`, `knee:G:K:R1`, `lad:U/R,U/R,...`,
    /// `sw:AT:<curve>;<curve>`.
    fn parse(spec: &str) -> Self {
        if let Some(rest) = spec.strip_prefix("sw:") {
            let (at, rest) = rest.split_once(':').expect("sw:AT:A;B");
            let (a, b) = rest.split_once(';').expect("sw:AT:A;B");
            return Self::Switch(
                at.parse().unwrap(),
                Box::new(Self::parse(a)),
                Box::new(Self::parse(b)),
            );
        }
        let parts: Vec<&str> = spec.split(':').collect();
        let num = |i: usize| -> f64 { parts[i].parse().expect("a number") };
        match parts[0] {
            "flat" => Self::Flat(num(1)),
            "geo" => Self::Geo(num(1), num(2)),
            "knee" => Self::Knee(num(1), num(2) as u64, num(3)),
            "lad" => Self::Ladder(
                parts[1]
                    .split(',')
                    .map(|point| {
                        let (u, r) = point.split_once('/').expect("U/R");
                        (u.parse().unwrap(), r.parse().unwrap())
                    })
                    .collect(),
            ),
            other => panic!("unknown curve {other}"),
        }
    }

    fn rate(&self, window: usize, units: u64) -> f64 {
        let lg = (units.max(1) as f64).log2();
        match self {
            Self::Flat(r) => *r,
            Self::Geo(g, r1) => r1 * g.powf(lg),
            Self::Knee(g, k, r1) => r1 * g.powf(lg.min((*k as f64).log2())),
            Self::Ladder(points) => {
                let (first, last) = (points[0], points[points.len() - 1]);
                if units <= first.0 {
                    return first.1;
                }
                for pair in points.windows(2) {
                    let (a, b) = (pair[0], pair[1]);
                    if units <= b.0 {
                        let (lo, hi) = ((a.0 as f64).log2(), (b.0 as f64).log2());
                        return a.1 + (b.1 - a.1) * (lg - lo) / (hi - lo);
                    }
                }
                last.1
            }
            Self::Switch(at, a, b) => match window < *at {
                true => a.rate(window, units),
                false => b.rate(window, units),
            },
        }
    }
}

/// One batch size's recorded series in a trace file.
struct Series {
    size: u64,
    /// Window time outside the batches, ms.
    overhead_ms: f64,
    /// Time-ordered ms per unit of each recorded batch.
    ms_per_unit: Vec<f64>,
    /// Start of each batch on the series' own clock, ms, and its total.
    starts_ms: Vec<f64>,
    /// Where this series starts reading, ms into it.
    offset_ms: f64,
}

/// A trace file: `first <ms>` (the first batch after a load), then per size
/// `size <units> ovh <ms> n <count>` and a line of ms-per-unit values.
/// Lines starting with `#` are comments.
struct Trace {
    series: Vec<Series>,
    first_ms: f64,
    /// Nonzero: every size reads its series at the pace of this size, so all
    /// sizes see the same host level at the same time. 0: each its own clock.
    shared_clock: f64,
}

impl Trace {
    fn load(path: &Path, rng: &mut Rng, min_batches: usize, same_start: bool, shared: f64) -> Self {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|err| panic!("trace {}: {err}", path.display()));
        let mut lines = text
            .lines()
            .filter(|l| !l.trim().is_empty() && !l.starts_with('#'));
        let (mut series, mut first_ms) = (Vec::new(), 0.0);
        let common = rng.unit();
        while let Some(line) = lines.next() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts[0] == "first" {
                first_ms = parts[1].parse().unwrap();
                continue;
            }
            let size: u64 = parts[1].parse().unwrap();
            let overhead_ms: f64 = parts[3].parse().unwrap();
            let ms_per_unit: Vec<f64> = lines
                .next()
                .expect("a series line")
                .split_whitespace()
                .map(|v| v.parse().unwrap())
                .collect();
            let own = rng.unit();
            if ms_per_unit.len() < min_batches {
                continue;
            }
            let mut starts_ms = Vec::with_capacity(ms_per_unit.len() + 1);
            let mut t = 0.0;
            for v in &ms_per_unit {
                starts_ms.push(t);
                t += v * size as f64;
            }
            starts_ms.push(t);
            let offset_ms = if same_start { common } else { own } * t;
            series.push(Series {
                size,
                overhead_ms,
                ms_per_unit,
                starts_ms,
                offset_ms,
            });
        }
        Self {
            series,
            first_ms,
            shared_clock: shared,
        }
    }

    /// The recorded size nearest `units` in log2.
    fn nearest(&self, units: u64) -> &Series {
        let lg = (units.max(1) as f64).log2();
        let distance = |s: &Series| ((s.size as f64).log2() - lg).abs();
        self.series
            .iter()
            .min_by(|a, b| distance(a).total_cmp(&distance(b)))
            .expect("a trace with at least one size")
    }

    /// ms a batch of `units` takes at simulated time `clock_ms`.
    fn batch_ms(&self, units: u64, clock_ms: f64) -> f64 {
        let s = self.nearest(units);
        let total = s.starts_ms[s.starts_ms.len() - 1];
        let pace = match self.shared_clock > 0.0 {
            true => s.size as f64 / self.shared_clock,
            false => 1.0,
        };
        let t = (clock_ms * pace + s.offset_ms) % total;
        let at = s.starts_ms.partition_point(|start| *start <= t);
        units as f64 * s.ms_per_unit[at.saturating_sub(1).min(s.ms_per_unit.len() - 1)]
    }
}

/// splitmix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A multiplicative noise factor: Gaussian with sd `amp` (floored at
    /// 0.3), or uniform within ±`amp`.
    fn noise(&mut self, amp: f64, gauss: bool) -> f64 {
        if amp <= 0.0 {
            return 1.0;
        }
        if gauss {
            let (u1, u2) = (self.unit().max(1e-12), self.unit());
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            (1.0 + amp * z).max(0.3)
        } else {
            1.0 + amp * (2.0 * self.unit() - 1.0)
        }
    }
}

/// A host that runs at `1 + amp` or `1 - amp` of its speed, each level held
/// for an exponentially distributed time of mean `hold_s`: one level at a
/// time for every batch size.
struct HostLevels {
    amp: f64,
    hold_ms: f64,
    slow: bool,
    until_ms: f64,
    rng: Rng,
}

impl HostLevels {
    fn factor(&mut self, clock_ms: f64) -> f64 {
        if self.amp <= 0.0 {
            return 1.0;
        }
        while clock_ms >= self.until_ms {
            self.slow = !self.slow;
            self.until_ms += -self.rng.unit().max(1e-12).ln() * self.hold_ms;
        }
        if self.slow {
            1.0 - self.amp
        } else {
            1.0 + self.amp
        }
    }
}

/// Where the replica runs.
#[derive(Clone, Copy, PartialEq)]
enum Device {
    /// A CUDA GPU on a host whose RAM the ledger also prices.
    GpuRam,
    /// A CUDA GPU with no host-RAM side.
    Gpu,
    /// Apple Silicon: one unified memory for the GPU and the host.
    Mac,
    /// The CPU device: host RAM is the working memory.
    Cpu,
}

impl Device {
    fn parse(spec: &str) -> Self {
        match spec {
            "gpu-ram" => Self::GpuRam,
            "gpu" => Self::Gpu,
            "mac" => Self::Mac,
            "cpu" => Self::Cpu,
            other => panic!("unknown device {other}"),
        }
    }

    fn key(self) -> &'static str {
        match self {
            Self::GpuRam | Self::Gpu => GPU,
            Self::Mac => crate::inferio::mps::DEVICE_KEY,
            Self::Cpu => cpu::DEVICE_KEY,
        }
    }

    fn store_env(self) -> StoreEnv {
        let (platform, backend) = match self {
            Self::GpuRam | Self::Gpu => ("linux", "cuda"),
            Self::Mac => ("macos", "mps"),
            Self::Cpu => ("linux", "cpu"),
        };
        StoreEnv {
            platform: platform.to_owned(),
            backend: backend.to_owned(),
            generator: "sizing-sim".to_owned(),
        }
    }

    /// A new process's ledger over a device of `total_mb`.
    fn ledger(self, total_mb: u64, mode: &str, store: &Arc<CalibrationStore>) -> Arc<VramLedger> {
        let profiles = Some(Arc::clone(store) as Arc<dyn CalibrationProfiles>);
        let inventory = match self {
            Self::Gpu => {
                return VramLedger::for_test_with(
                    &[(GPU, "TEST 9000", total_mb)],
                    budget(mode),
                    profiles,
                );
            }
            Self::GpuRam => GpuInventory::known(vec![nvidia(0, GPU, "TEST 9000", total_mb)])
                .with_cpu(CPU_RAM_MB, cpu::MemRoots::default()),
            // Metal's working set, the device total, is three quarters of RAM.
            Self::Mac => GpuInventory::known_mps(total_mb / 3 * 4),
            Self::Cpu => GpuInventory::known_cpu(total_mb),
        };
        let mut ledger = VramLedger::new(&inventory, budget(mode).into(), profiles);
        Arc::get_mut(&mut ledger)
            .expect("not shared yet")
            .probe_external = false;
        ledger
    }

    /// A loaded worker's telemetry, `base_mb` over the device's idle state.
    fn worker(self, base_mb: u64, total_mb: u64) -> TelemetryHandle {
        let handle = match self {
            Self::GpuRam | Self::Gpu => loaded_on(GPU, Some(base_mb), Some(0)),
            Self::Mac => loaded_mps(Some(total_mb)),
            Self::Cpu => loaded_cpu(Some(total_mb)),
        };
        {
            let mut telemetry = handle.lock().unwrap();
            let load = &mut telemetry.load.as_mut().expect("a load report").value;
            load.base_mb = Some(base_mb);
            load.dtype = Some("fp16".to_owned());
            load.gpu_arch.get_or_insert_with(|| "sim".to_owned());
            if self == Self::GpuRam {
                load.rss_at_load_mb = Some(RSS_AT_LOAD_MB);
            }
        }
        handle
    }
}

/// The memory budget every device is configured with. A sizing mode
/// (`balanced`, `throughput`) is applied here once the ledger has one.
fn budget(_mode: &str) -> VramBudget {
    no_margin()
}

/// One scenario line. Every key is optional; defaults in [`Scenario::parse`].
struct Scenario {
    name: String,
    device: Device,
    mode: String,
    /// MiB free for this model beside its own pool.
    room: u64,
    /// MiB of the device above the worker's base.
    total: u64,
    base: u64,
    seed: u32,
    /// MiB of pool per unit of the largest batch run.
    mb_per_unit: u64,
    /// Peak resident-set growth per unit, host-RAM side only.
    rss_per_unit: u64,
    /// Stop after this many windows, items or simulated seconds.
    windows: usize,
    items: u64,
    secs: f64,
    /// Batches per full window (a fraction adds a short batch).
    depth: f64,
    curve: Curve,
    noise: f64,
    gauss: bool,
    levels: Option<(f64, f64)>,
    noise_seed: u64,
    /// `full`, `first1` (one unit in the first window) or units per window.
    queue: String,
    /// The queue holds three times the size run two windows back, at least
    /// `queue_floor` units: the caller learns of a new size two windows late.
    lag: bool,
    queue_floor: u64,
    /// A user batch cap.
    cap: Option<u32>,
    /// Window time outside the batches when no trace is given, ms.
    overhead_ms: f64,
    trace: Option<PathBuf>,
    trace_seed: u64,
    trace_min: usize,
    trace_same_start: bool,
    trace_shared_clock: f64,
    /// A stored local row: `a:<anchor>[:k:<working>[:f:<failed>:w:<wait>]]`.
    profile: Option<String>,
    /// A shipped row's anchor.
    shipped: u64,
    starts: usize,
    /// Every start is a new process (true) or a new job in the same one.
    restart: bool,
    /// Room changes: `(window, room)`.
    room_schedule: Vec<(usize, u64)>,
    /// Per-window columns in the output, run-length encoded with `compact`.
    verbose: bool,
    compact: bool,
}

impl Scenario {
    fn parse(line: &str, traces: &Path) -> Self {
        let tokens: Vec<(&str, &str)> = line
            .split_whitespace()
            .filter_map(|t| t.split_once('='))
            .collect();
        let get = |key: &str, default: &str| -> String {
            tokens
                .iter()
                .find(|(k, _)| *k == key)
                .map_or(default, |(_, v)| *v)
                .to_owned()
        };
        let num = |key: &str, default: &str| -> f64 { get(key, default).parse().expect(key) };
        let room = num("room", "15700") as u64;
        Self {
            name: get("name", "x"),
            device: Device::parse(&get("dev", "gpu-ram")),
            mode: get("mode", "balanced"),
            room,
            total: num("total", &room.to_string()) as u64,
            base: num("base", "554") as u64,
            seed: num("seed", "64") as u32,
            mb_per_unit: num("pu", "82") as u64,
            rss_per_unit: num("rsspu", "10") as u64,
            windows: num("win", "400") as usize,
            items: num("items", "0") as u64,
            secs: num("secs", "0"),
            depth: num("depth", "3"),
            curve: Curve::parse(&get("curve", "flat:22")),
            noise: num("noise", "0"),
            gauss: get("ndist", "u") == "g",
            levels: get("levels", "")
                .split_once(':')
                .map(|(a, h)| (a.parse().unwrap(), h.parse().unwrap())),
            noise_seed: num("nseed", "1") as u64,
            queue: get("queue", "full"),
            lag: get("lag", "0") == "1",
            queue_floor: num("qfloor", "48") as u64,
            cap: Some(num("cap", "0") as u32).filter(|cap| *cap > 0),
            overhead_ms: num("ovh", "0"),
            trace: Some(get("trace", ""))
                .filter(|t| !t.is_empty())
                .map(|t| traces.join(t)),
            trace_seed: num("tseed", "1") as u64,
            trace_min: num("tmin", "8") as usize,
            trace_same_start: get("tshared", "0") == "1",
            trace_shared_clock: num("tref", "0"),
            profile: Some(get("prof", "none")).filter(|p| p != "none"),
            shipped: num("ship", "0") as u64,
            starts: num("starts", "1") as usize,
            restart: get("restart", "1") == "1",
            room_schedule: get("roomsched", "")
                .split(',')
                .filter_map(|p| p.split_once(':'))
                .map(|(w, r)| (w.parse().unwrap(), r.parse().unwrap()))
                .collect(),
            verbose: get("v", "0") == "1",
            compact: get("compact", "0") == "1",
        }
    }

    /// Units the queue holds for the next window.
    fn window_units(&self, window: usize, ran: &[u64], items_left: Option<u64>) -> u64 {
        let mut units = match self.queue.as_str() {
            "full" => u64::MAX,
            "first1" if window == 0 => 1,
            "first1" => u64::MAX,
            n => n.parse().expect("queue"),
        };
        if self.lag {
            let before = if ran.len() >= 2 {
                ran[ran.len() - 2]
            } else {
                1
            };
            units = units.min((3 * before).max(self.queue_floor));
        }
        items_left.map_or(units, |left| units.min(left))
    }
}

/// A row as the store's own record path writes it.
fn stored_row(anchor: u64, working: Option<u64>, mb_per_unit: u64, base_mb: u64) -> ProfileUpdate {
    ProfileUpdate {
        inference_id: "g/v".to_owned(),
        epoch: 1,
        arch: ARCH.to_owned(),
        gpu_name: "TEST 9000".to_owned(),
        torch: "2.7.1+cu128".to_owned(),
        dtype: "fp16".to_owned(),
        unit: "item",
        aggregation: "count",
        base_mb,
        base_method: Some("nvml".to_owned()),
        dtype_method: None,
        slope_mb_per_unit: mb_per_unit as f64,
        residual_mb: 0.0,
        samples: 20,
        knee_units: working,
        knee_trials: Default::default(),
        knee_rates: Vec::new(),
        max_units_measured: anchor,
        local_samples: 20,
        ring: [anchor / 4, anchor / 2, anchor]
            .iter()
            .filter(|u| **u > 0)
            .map(|u| FitSample {
                units: *u,
                delta_mb: mb_per_unit * *u,
            })
            .collect(),
    }
}

fn store_at(dir: &Path, shipped: Option<PathBuf>, env: StoreEnv) -> Arc<CalibrationStore> {
    let paths = StorePaths {
        shipped_dirs: shipped.into_iter().collect(),
        local_path: dir.join("calibration.toml"),
    };
    CalibrationStore::with_debounce(paths, env, Duration::ZERO)
}

/// The local store's working size (-1: none) and retest wait.
fn stored_working(path: &Path) -> (i64, i64) {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let doc: toml::Value = toml::from_str(&text).expect("the store is TOML");
    let rows = doc.get("profile").and_then(|rows| rows.as_array());
    let row = rows.and_then(|rows| {
        rows.iter()
            .find(|r| r.get("inference_id").and_then(|id| id.as_str()) == Some("g/v"))
    });
    let field = |key: &str| row.and_then(|r| r.get(key)).and_then(|v| v.as_integer());
    (
        field("knee_units").unwrap_or(-1),
        field("knee_retest_after").unwrap_or(0),
    )
}

/// Run-length encoding: `64x12,128,64x3`.
fn run_lengths(values: &[u64]) -> String {
    let mut out = Vec::new();
    let mut i = 0;
    while i < values.len() {
        let j = i + values[i..].iter().take_while(|v| **v == values[i]).count();
        out.push(match j - i {
            1 => values[i].to_string(),
            n => format!("{}x{n}", values[i]),
        });
        i = j;
    }
    out.join(",")
}

/// The batches a window of `window_units` runs at a budget of `units`.
fn batches(sc: &Scenario, window_units: u64, units: u64, single: bool) -> Vec<u64> {
    if single {
        return vec![units];
    }
    if window_units != u64::MAX && window_units <= units.saturating_mul(3) {
        let (mut left, mut sizes) = (window_units.max(1), Vec::new());
        while left > 0 && sizes.len() < 3 {
            sizes.push(left.min(units));
            left -= left.min(units);
        }
        return sizes;
    }
    let mut sizes = vec![units; sc.depth.floor() as usize];
    let frac = sc.depth.fract();
    if frac > 0.0 {
        sizes.push(((units as f64 * frac).round() as u64).max(1));
    }
    sizes
}

fn run_scenario(line: &str, traces: &Path, out: &mut impl std::io::Write) {
    let sc = Scenario::parse(line, traces);
    let mut trace_rng = Rng(sc.trace_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5151);
    let trace = sc.trace.as_ref().map(|path| {
        Trace::load(
            path,
            &mut trace_rng,
            sc.trace_min,
            sc.trace_same_start,
            sc.trace_shared_clock,
        )
    });
    let root = tempfile::tempdir().unwrap();
    let env = sc.device.store_env();
    let gpu_only = |what: &str| {
        assert!(
            matches!(sc.device, Device::GpuRam | Device::Gpu),
            "{what} needs a CUDA device"
        )
    };
    if sc.shipped > 0 {
        gpu_only("ship");
        let scratch = store_at(&root.path().join("scratch"), None, env.clone());
        scratch.record(stored_row(sc.shipped, None, sc.mb_per_unit, sc.base));
        scratch.write_pending();
        std::fs::create_dir_all(root.path().join("shipped")).unwrap();
        std::fs::copy(
            root.path().join("scratch/calibration.toml"),
            root.path().join("shipped/x.toml"),
        )
        .unwrap();
    }
    let shipped = (sc.shipped > 0).then(|| root.path().join("shipped"));
    let store = store_at(&root.path().join("local"), shipped, env);
    if let Some(profile) = &sc.profile {
        gpu_only("prof");
        let parts: Vec<&str> = profile.split(':').collect();
        let mut row = stored_row(
            parts[1].parse().unwrap(),
            parts.get(3).map(|k| k.parse().unwrap()),
            sc.mb_per_unit,
            554,
        );
        if let (Some(failed), Some(wait)) = (parts.get(5), parts.get(7)) {
            row.knee_trials = TrialCadence {
                failed: failed.parse().unwrap(),
                retest_after: wait.parse().unwrap(),
            };
        }
        store.record(row);
        store.write_pending();
    }

    let device_mb = sc.base + sc.total;
    let mut rng = Rng(sc.noise_seed.wrapping_mul(0x1234_5678_9ABC_DEF1) ^ 0xDEAD_BEEF);
    let mut levels = sc.levels.map(|(amp, hold_s)| HostLevels {
        amp,
        hold_ms: hold_s * 1000.0,
        slow: false,
        until_ms: 0.0,
        rng: Rng(sc.noise_seed ^ 0x1E7E15),
    });
    let mut clock_ms = 0f64;
    let mut ledger = sc.device.ledger(device_mb, &sc.mode, &store);
    for start in 0..sc.starts {
        if sc.restart && start > 0 {
            ledger = sc.device.ledger(device_mb, &sc.mode, &store);
        }
        let handle = sc.device.worker(sc.base, device_mb);
        let admission = ledger
            .register_worker("g/v", item_cost(sc.seed), &handle, Some(sc.device.key()))
            .expect("admitted");
        if sc.device == Device::GpuRam {
            ledger.record_free_for_test(cpu::DEVICE_KEY, 45_000 + CPU_RAM_MB / 10);
        }
        let (mut pool, mut pool_peak, mut room) = (0u64, 0u64, sc.room);
        let (mut items, mut true_s, mut ooms, mut trims) = (0u64, 0f64, 0u32, 0u32);
        let (mut ran, mut working, mut window_ms, mut window_items, mut pools) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut job_ms, mut first_batch, mut trial_windows) = (0f64, true, 0u32);
        for w in 0..sc.windows {
            if (sc.items > 0 && items >= sc.items) || (sc.secs > 0.0 && job_ms >= sc.secs * 1000.0)
            {
                break;
            }
            if let Some((_, r)) = sc.room_schedule.iter().find(|(at, _)| *at == w) {
                room = *r;
            }
            ledger.record_free_for_test(sc.device.key(), room.saturating_sub(pool));
            let window_units = sc.window_units(w, &ran, (sc.items > 0).then(|| sc.items - items));
            let working_before = ledger_row(&ledger).knee_units.unwrap_or(0);
            let token = admission
                .request_grant(window_units, sc.cap, 1, 0)
                .expect("granted");
            let grant = *token.grant();
            let units = grant
                .unit_budget
                .min(grant.user_cap_items.map_or(u64::MAX, u64::from));
            // The item cap of a cold load holds the window to one batch.
            let single = grant.user_cap_items == Some(1) && sc.cap != Some(1);
            let mut out_ms = trace
                .as_ref()
                .map_or(sc.overhead_ms, |t| t.nearest(units).overhead_ms);
            let (items_before, mut oom, mut measurements) = (items, false, Vec::new());
            for size in batches(&sc, window_units, units, single) {
                let need = sc.mb_per_unit * size;
                if need > room {
                    oom = true;
                    break;
                }
                let before = pool;
                pool = pool.max(need);
                pool_peak = pool_peak.max(pool);
                let level = levels.as_mut().map_or(1.0, |l| l.factor(clock_ms));
                let true_rate = sc.curve.rate(w, size);
                let seen_rate = true_rate * level * rng.noise(sc.noise, sc.gauss);
                items += size;
                true_s += size as f64 / true_rate;
                let ms = match &trace {
                    // The first batch after a load takes at least the recorded first batch.
                    Some(t) if first_batch => t.batch_ms(size, clock_ms).max(t.first_ms),
                    Some(t) => t.batch_ms(size, clock_ms),
                    None => size as f64 * 1000.0 / seen_rate,
                };
                first_batch = false;
                clock_ms += ms;
                out_ms += ms;
                let mut m = BatchMeasurement {
                    reserved_before_mb: Some(before),
                    reserved_after_mb: Some(pool),
                    allocated_before_mb: Some(0),
                    peak_allocated_mb: Some(need),
                    duration_ms: Some(ms),
                    ..measurement(size, 0, pool)
                };
                if sc.device == Device::GpuRam {
                    m.peak_rss_mb = Some(RSS_AT_LOAD_MB + sc.rss_per_unit * size);
                    m.rss_after_mb = Some(RSS_AT_LOAD_MB);
                }
                measurements.push(m);
            }
            // The ledger's clock sees the window out for its batches and the
            // time outside them.
            if sc.overhead_ms > 0.0 || trace.is_some() {
                token.age_for_test(out_ms);
            }
            if let Some(t) = &trace {
                clock_ms += t.nearest(units).overhead_ms;
            }
            true_s += sc.overhead_ms / 1000.0;
            job_ms += out_ms;
            handle.lock().unwrap().record_measurements(measurements);
            token.finish(WindowOutcome::Responded {
                oom: oom.then_some(ErrorFrameOom::Prose),
            });
            ooms += oom as u32;
            if admission.take_trial_trim() {
                pool = 0;
                trims += 1;
            }
            trial_windows += (working_before > 0
                && units != working_before
                && grant.user_cap_items.is_none()) as u32;
            ran.push(units);
            window_ms.push(out_ms.round() as u64);
            window_items.push(items - items_before);
            pools.push(pool);
            working.push(ledger_row(&ledger).knee_units.unwrap_or(0));
        }
        let pool_last = pool;
        // The job is over: the dispatcher tells the ledger the queue is dry.
        admission.note_demand(0);
        if admission.take_trial_trim() {
            pool = 0;
            trims += 1;
        }
        store.write_pending();
        let row = ledger_row(&ledger);
        let (stored, wait) = stored_working(&root.path().join("local/calibration.toml"));
        let pool_ms: f64 = pools
            .iter()
            .zip(&window_ms)
            .map(|(p, ms)| (*p * *ms) as f64)
            .sum();
        let opening = working.iter().copied().find(|w| *w > 0).unwrap_or(0);
        // `ms`: the job's wall time; `true_s`: its time at the curve's own rates.
        let mut line = format!(
            "name={} start={start} mode={} windows={} items={items} ms={job_ms:.0} true_s={true_s:.3} \
             W={} open={opening} stored={stored} wait={wait} trial={} trialw={trial_windows} \
             poolmean={:.0} poolpeak={pool_peak} poollast={pool_last} poolend={pool} oom={ooms} trims={trims}",
            sc.name,
            sc.mode,
            ran.len(),
            row.knee_units.map_or(-1, |k| k as i64),
            row.trial_units.map_or(-1, |k| k as i64),
            pool_ms / job_ms.max(1.0),
        );
        if sc.verbose {
            let join = |v: &[u64]| match sc.compact {
                true => run_lengths(v),
                false => v.iter().map(u64::to_string).collect::<Vec<_>>().join(","),
            };
            line += &format!(
                " budgets={} working={} wms={} witems={} pools={}",
                join(&ran),
                join(&working),
                join(&window_ms),
                join(&window_items),
                join(&pools)
            );
        }
        writeln!(out, "{line}").unwrap();
        drop(admission);
    }
}

fn ledger_row(ledger: &Arc<VramLedger>) -> crate::inferio::ledger::health::LedgerWorkerHealth {
    ledger
        .health()
        .into_iter()
        .flat_map(|gpu| gpu.workers)
        .find(|worker| worker.inference_id == "g/v")
        .expect("a resident replica")
}

/// Every scenario of `spec`, one output line per start; a scenario that
/// panics yields one `panic` line instead.
fn run_spec(spec: &str, traces: &Path, out: &mut impl std::io::Write) {
    for line in spec.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut buf = Vec::new();
            run_scenario(line, traces, &mut buf);
            buf
        }));
        match result {
            Ok(buf) => out.write_all(&buf).unwrap(),
            Err(_) => writeln!(out, "panic in: {line}").unwrap(),
        }
    }
}

#[test]
#[ignore = "simulator: run with SIZING_SPEC, SIZING_TRACES and SIZING_OUT set"]
fn sizing_sim() {
    let spec = std::env::var("SIZING_SPEC").expect("SIZING_SPEC: a scenario file");
    let out = std::env::var("SIZING_OUT").expect("SIZING_OUT: the output file");
    let traces = PathBuf::from(std::env::var("SIZING_TRACES").unwrap_or_default());
    let text = std::fs::read_to_string(spec).unwrap();
    let mut file = std::io::BufWriter::new(std::fs::File::create(out).unwrap());
    run_spec(&text, &traces, &mut file);
    file.flush().unwrap();
}

/// Every device runs a scenario to the end and writes one line per start.
#[test]
fn every_device_simulates_one_line_per_start() {
    let mut spec = String::new();
    for dev in ["gpu-ram", "gpu", "mac", "cpu"] {
        spec += &format!(
            "name={dev} dev={dev} curve=knee:1.3:128:8 noise=0.05 win=60 starts=2 levels=0.1:5\n"
        );
    }
    let mut out = Vec::new();
    run_spec(&spec, Path::new(""), &mut out);
    let out = String::from_utf8(out).unwrap();
    assert_eq!(out.lines().count(), 8, "{out}");
    assert!(
        out.lines()
            .all(|l| l.starts_with("name=") && l.contains(" windows=60 ")),
        "{out}"
    );
}
