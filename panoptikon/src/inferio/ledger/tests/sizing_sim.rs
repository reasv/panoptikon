//! Closed-loop batch-size simulator: the real ledger sizes every window, a
//! rate curve or a recorded trace says how long its batches take.
//!
//! `sizing_sim` is ignored by default. It reads one scenario per line from
//! `SIZING_SPEC` (`key=value` tokens, see [`Scenario::parse`]), resolves
//! trace names against `SIZING_TRACES`, and writes one line per process start
//! to `SIZING_OUT`. `tools/calibration-protocol/sizing_table.py` builds the
//! specs and reads the output.
//!
//! The loop stands in for the dispatcher and the worker: the window is taken
//! from the queue within the ledger's window target, the grant is asked with
//! the window's and the queue's counts, and the worker packs the window
//! within the grant, shrinks a batch to live memory as its clamp does and
//! reports memory the way the worker on that device does.
use super::*;
use crate::inferio::calibration::{CalibrationProfile, TrialCadence};
use crate::inferio::dispatch::in_flight_target_units;
use crate::inferio::gpu::GpuInventory;
use crate::inferio::worker::ClampReport;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A GPU worker's resident set at load, before any batch.
const RSS_AT_LOAD_MB: u64 = 2_000;
/// Requests behind a window when the queue has no end in sight.
const DEEP_QUEUE: u64 = 1_000_000;
/// The worker releases its pool once the grant stays below this share of the
/// releasable pool for [`SHRINK_WINDOWS`] windows in a row.
const SHRINK_RATIO: f64 = 0.8;
const SHRINK_WINDOWS: u32 = 2;
const SHRINK_BLIND_SLACK_MB: u64 = 256;
/// The least share of the grant a batch carries to report that the next item
/// would not have fitted.
const NEXT_OVER_BUDGET_MIN_RATIO: f64 = 0.5;

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
    /// Window time outside the batches, ms; `None` where no window measured it.
    overhead_ms: Option<f64>,
    /// Time-ordered ms per unit of each recorded batch.
    ms_per_unit: Vec<f64>,
    /// Start of each batch on the series' own clock, ms, and its total.
    starts_ms: Vec<f64>,
    /// Where this series starts reading, ms into it.
    offset_ms: f64,
    mean: f64,
}

/// A trace file: `first <ms>` (the first batch after a load), `warm <ms>...`
/// (what each next batch after a load takes on top of its own time), then per
/// size `size <units> ovh <ms>|- n <count>` and a line of ms-per-unit values.
/// Lines starting with `#` are comments.
struct Trace {
    series: Vec<Series>,
    first_ms: f64,
    warm_ms: Vec<f64>,
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
        let (mut series, mut first_ms, mut warm_ms) = (Vec::new(), 0.0, Vec::new());
        let common = rng.unit();
        while let Some(line) = lines.next() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            match parts[0] {
                "first" => first_ms = parts[1].parse().unwrap(),
                "warm" => warm_ms = parts[1..].iter().map(|v| v.parse().unwrap()).collect(),
                _ => {
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
                    let size: u64 = parts[1].parse().unwrap();
                    let mut starts_ms = Vec::with_capacity(ms_per_unit.len() + 1);
                    let mut t = 0.0;
                    for v in &ms_per_unit {
                        starts_ms.push(t);
                        t += v * size as f64;
                    }
                    starts_ms.push(t);
                    series.push(Series {
                        size,
                        overhead_ms: parts[3].parse().ok(),
                        mean: ms_per_unit.iter().sum::<f64>() / ms_per_unit.len() as f64,
                        offset_ms: if same_start { common } else { own } * t,
                        ms_per_unit,
                        starts_ms,
                    });
                }
            }
        }
        assert!(!series.is_empty(), "{}: no size kept", path.display());
        Self {
            series,
            first_ms,
            warm_ms,
            shared_clock: shared,
        }
    }

    /// One recorded series replayed at every size.
    fn is_flat(&self) -> bool {
        self.series
            .windows(2)
            .all(|pair| pair[0].ms_per_unit == pair[1].ms_per_unit)
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

    /// Window time outside the batches: linear in units between measured
    /// sizes, the smallest's below them, and per unit of the largest above.
    fn overhead_ms(&self, units: u64) -> f64 {
        let points: Vec<(f64, f64)> = self
            .series
            .iter()
            .filter_map(|s| s.overhead_ms.map(|ms| (s.size as f64, ms)))
            .collect();
        let (Some(first), Some(last)) = (points.first(), points.last()) else {
            return 0.0;
        };
        let u = units as f64;
        if u <= first.0 {
            return first.1;
        }
        for pair in points.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            if u <= b.0 {
                return a.1 + (b.1 - a.1) * (u - a.0) / (b.0 - a.0);
            }
        }
        last.1 * u / last.0
    }

    /// The recorded ms per unit at simulated time `clock_ms` for `units`, and
    /// that series' mean.
    fn ms_per_unit(&self, units: u64, clock_ms: f64) -> (f64, f64) {
        let s = self.nearest(units);
        let total = s.starts_ms[s.starts_ms.len() - 1];
        let pace = match self.shared_clock > 0.0 {
            true => s.size as f64 / self.shared_clock,
            false => 1.0,
        };
        let t = (clock_ms * pace + s.offset_ms) % total;
        let at = s.starts_ms.partition_point(|start| *start <= t);
        let value = s.ms_per_unit[at.saturating_sub(1).min(s.ms_per_unit.len() - 1)];
        (value, s.mean)
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
    /// A GPU that shares the host's RAM (an APU).
    Apu,
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
            "apu" => Self::Apu,
            "mac" => Self::Mac,
            "cpu" => Self::Cpu,
            other => panic!("unknown device {other}"),
        }
    }

    fn key(self) -> &'static str {
        match self {
            Self::GpuRam | Self::Gpu | Self::Apu => GPU,
            Self::Mac => crate::inferio::mps::DEVICE_KEY,
            Self::Cpu => cpu::DEVICE_KEY,
        }
    }

    /// The source a free reading on this device names.
    fn free_source(self) -> &'static str {
        match self {
            Self::GpuRam | Self::Gpu => "nvml",
            Self::Apu => "amdgpu-sysfs",
            Self::Mac => "mps",
            Self::Cpu => "ram",
        }
    }

    fn store_env(self) -> StoreEnv {
        let (platform, backend) = match self {
            Self::GpuRam | Self::Gpu => ("linux", "cuda"),
            Self::Apu => ("linux", "rocm"),
            Self::Mac => ("macos", "mps"),
            Self::Cpu => ("linux", "cpu"),
        };
        StoreEnv {
            platform: platform.to_owned(),
            backend: backend.to_owned(),
            generator: "sizing-sim".to_owned(),
        }
    }

    /// A new process's ledger over a device of `total_mb`, with the default
    /// memory budget.
    fn ledger(
        self,
        total_mb: u64,
        host_mb: u64,
        mode: &str,
        store: &Arc<CalibrationStore>,
    ) -> Arc<VramLedger> {
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
                .with_cpu(host_mb, cpu::MemRoots::default()),
            Self::Apu => GpuInventory::known(vec![crate::inferio::gpu::GpuInfo {
                unified_ram_mb: Some(total_mb),
                ..nvidia(0, GPU, "TEST APU", total_mb)
            }]),
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
            Self::GpuRam | Self::Gpu | Self::Apu => loaded_on(GPU, Some(base_mb), Some(0)),
            Self::Mac => loaded_mps(Some(total_mb)),
            Self::Cpu => loaded_cpu(Some(total_mb)),
        };
        {
            let mut telemetry = handle.lock().unwrap();
            let load = &mut telemetry.load.as_mut().expect("a load report").value;
            load.base_mb = Some(base_mb);
            load.dtype = Some("fp16".to_owned());
            load.gpu_arch.get_or_insert_with(|| "sim".to_owned());
            match self {
                Self::GpuRam => load.rss_at_load_mb = Some(RSS_AT_LOAD_MB),
                // The CPU worker's pool figures are its resident set.
                Self::Cpu => {
                    load.reserved_at_load_mb = Some(base_mb);
                    load.allocated_at_load_mb = Some(base_mb);
                }
                _ => {}
            }
        }
        handle
    }
}

/// The memory budget every device is configured with: the shipped default.
/// A sizing mode (`balanced`, `throughput`) is applied here once the ledger
/// has one.
fn budget(_mode: &str) -> VramBudget {
    VramBudget::default()
}

/// Where a design hands the ledger the items its job has left; the ledger
/// takes none today. `None`: the job has no end in sight.
fn remaining_work(_admission: &Admission, _items_left: Option<u64>) {}

/// `(window, value)` pairs from `w:v,w:v`.
fn schedule<T>(spec: &str, value: impl Fn(&str) -> T) -> Vec<(usize, T)> {
    spec.split(',')
        .filter_map(|p| p.split_once(':'))
        .map(|(w, v)| (w.parse().expect("a window"), value(v)))
        .collect()
}

fn pressure(level: &str) -> mps::MemoryPressure {
    match level {
        "normal" => mps::MemoryPressure::Normal,
        "warning" => mps::MemoryPressure::Warning,
        "paging" => mps::MemoryPressure::Paging,
        "critical" => mps::MemoryPressure::Critical,
        other => panic!("unknown pressure {other}"),
    }
}

/// Every key a scenario line may carry.
const KEYS: &str = "name dev mode room total base seed pu rsspu win items secs curve curve2 noise \
    ndist levels nseed queue lag qmul qfloor cap fixed ovh trace trace2 swstart \
    tseed tmin tshared tref tnoise prof ship starts restart roomsched roomlate \
    hostram hostfree hostsched pressure die cost upi v compact";

/// One scenario line. Every key is optional; defaults in [`Scenario::parse`].
struct Scenario {
    name: String,
    device: Device,
    mode: String,
    /// MiB this model may hold, its pool included. Another process's growth
    /// lowers it, but cannot take the pool the worker holds.
    room: u64,
    /// MiB of the device above the worker's base.
    total: u64,
    base: u64,
    seed: u32,
    /// MiB of pool per unit of the largest batch run.
    mb_per_unit: f64,
    /// Resident-set growth per unit of a GPU worker, MiB.
    rss_per_unit: f64,
    /// Stop after this many windows, items or simulated seconds.
    windows: usize,
    items: u64,
    secs: f64,
    /// The rate curve, and the one from start `switch_start` on.
    curve: Curve,
    curve2: Option<Curve>,
    noise: f64,
    gauss: bool,
    levels: Option<(f64, f64)>,
    noise_seed: u64,
    /// `full`, `first1` (one item in the first window) or items per window.
    queue: String,
    /// Nonzero: the queue holds `qmul` times the size run `lag` windows back,
    /// at least `queue_floor` units: the caller learns of a new size late.
    lag: usize,
    qmul: f64,
    queue_floor: u64,
    /// A user batch cap, items.
    cap: Option<u32>,
    /// Run this many units every window, whatever the ledger says.
    fixed: Option<u64>,
    /// Window time outside the batches when no trace gives it, ms.
    overhead_ms: f64,
    trace: Option<PathBuf>,
    trace2: Option<PathBuf>,
    switch_start: usize,
    trace_seed: u64,
    trace_min: usize,
    trace_same_start: bool,
    trace_shared_clock: f64,
    /// The trace gives only the noise around its mean; the curve the rate.
    trace_noise: bool,
    /// A stored local row: `a:<anchor>[:k:<working>[:f:<failed>:w:<wait>]]`.
    profile: Option<String>,
    /// A shipped row's anchor.
    shipped: u64,
    starts: usize,
    /// Every start is a new process (true) or a new job in the same one.
    restart: bool,
    /// Room changes: `(window, room)`; with `room_late` a change lands after
    /// the window's grant, so the ledger's free reading is stale.
    room_schedule: Vec<(usize, u64)>,
    room_late: bool,
    /// Host RAM and its free MiB beside our worker, with changes.
    host_ram: u64,
    host_free: u64,
    host_schedule: Vec<(usize, u64)>,
    pressure: Vec<(usize, mps::MemoryPressure)>,
    /// Windows in which the worker process dies.
    die: Vec<usize>,
    cost: CostUnit,
    /// Units per item, uniform in `lo..=hi`.
    upi: (u64, u64),
    /// Per-window columns in the output, run-length encoded with `compact`.
    verbose: bool,
    compact: bool,
}

impl Scenario {
    fn parse(line: &str, traces: &Path) -> Self {
        let tokens: Vec<(&str, &str)> = line
            .split_whitespace()
            .map(|t| {
                t.split_once('=')
                    .unwrap_or_else(|| panic!("not key=value: {t}"))
            })
            .collect();
        for (key, _) in &tokens {
            assert!(
                KEYS.split_whitespace().any(|k| k == *key),
                "unknown key {key}"
            );
        }
        let given = |key: &str| tokens.iter().any(|(k, _)| *k == key);
        let get = |key: &str, default: &str| -> String {
            tokens
                .iter()
                .find(|(k, _)| *k == key)
                .map_or(default, |(_, v)| *v)
                .to_owned()
        };
        let num = |key: &str, default: &str| -> f64 { get(key, default).parse().expect(key) };
        let path = |key: &str| {
            Some(get(key, ""))
                .filter(|t| !t.is_empty())
                .map(|t| traces.join(t))
        };
        let trace_noise = get("tnoise", "0") == "1";
        if given("trace") && !trace_noise {
            for key in ["curve", "curve2", "noise", "ndist", "ovh"] {
                assert!(
                    !given(key),
                    "{key} means nothing with a trace (tnoise=1 mixes them)"
                );
            }
        }
        let room = num("room", "15700") as u64;
        let host_ram = num("hostram", &CPU_RAM_MB.to_string()) as u64;
        let host_free = (45_000 + host_ram / 10).min(host_ram * 4 / 5);
        let upi = get("upi", "1");
        let (lo, hi) = upi.split_once(':').unwrap_or((&upi, &upi));
        Self {
            name: get("name", "x"),
            device: Device::parse(&get("dev", "gpu-ram")),
            mode: get("mode", "balanced"),
            room,
            total: num("total", &room.to_string()) as u64,
            base: num("base", "554") as u64,
            seed: num("seed", "64") as u32,
            mb_per_unit: num("pu", "82"),
            rss_per_unit: num("rsspu", "10"),
            windows: num("win", "400") as usize,
            items: num("items", "0") as u64,
            secs: num("secs", "0"),
            curve: Curve::parse(&get("curve", "flat:22")),
            curve2: Some(get("curve2", ""))
                .filter(|c| !c.is_empty())
                .map(|c| Curve::parse(&c)),
            noise: num("noise", "0"),
            gauss: get("ndist", "u") == "g",
            levels: get("levels", "")
                .split_once(':')
                .map(|(a, h)| (a.parse().unwrap(), h.parse().unwrap())),
            noise_seed: num("nseed", "1") as u64,
            queue: get("queue", "full"),
            lag: num("lag", "0") as usize,
            qmul: num("qmul", "3"),
            queue_floor: num("qfloor", "48") as u64,
            cap: Some(num("cap", "0") as u32).filter(|cap| *cap > 0),
            fixed: Some(num("fixed", "0") as u64).filter(|units| *units > 0),
            overhead_ms: num("ovh", "0"),
            trace: path("trace"),
            trace2: path("trace2"),
            switch_start: num("swstart", "0") as usize,
            trace_seed: num("tseed", "1") as u64,
            trace_min: num("tmin", "8") as usize,
            trace_same_start: get("tshared", "0") == "1",
            trace_shared_clock: num("tref", "0"),
            trace_noise,
            profile: Some(get("prof", "none")).filter(|p| p != "none"),
            shipped: num("ship", "0") as u64,
            starts: num("starts", "1") as usize,
            restart: get("restart", "1") == "1",
            room_schedule: schedule(&get("roomsched", ""), |r| r.parse().unwrap()),
            room_late: get("roomlate", "0") == "1",
            host_ram,
            host_free: num("hostfree", &host_free.to_string()) as u64,
            host_schedule: schedule(&get("hostsched", ""), |r| r.parse().unwrap()),
            pressure: schedule(&get("pressure", ""), pressure),
            die: get("die", "")
                .split(',')
                .filter(|w| !w.is_empty())
                .map(|w| w.parse().unwrap())
                .collect(),
            cost: match get("cost", "item").as_str() {
                "item" => CostUnit::Item,
                "token" => CostUnit::Token,
                "pixel" => CostUnit::Pixel,
                other => panic!("unknown cost {other}"),
            },
            upi: (lo.parse().unwrap(), hi.parse().unwrap()),
            verbose: get("v", "0") == "1",
            compact: get("compact", "0") == "1",
        }
    }

    /// The store's names for the cost unit and aggregation.
    fn cost_names(&self) -> (&'static str, &'static str) {
        match self.cost {
            CostUnit::Token => ("token", "max-times-count"),
            CostUnit::Pixel => ("pixel", "sum"),
            _ => ("item", "count"),
        }
    }

    fn cost_dimension(&self) -> CostDimension {
        let aggregation = match self.cost {
            CostUnit::Token => CostAggregation::MaxTimesCount,
            CostUnit::Pixel => CostAggregation::Sum,
            _ => CostAggregation::Count,
        };
        CostDimension {
            unit: self.cost,
            aggregation: Some(aggregation),
            ..item_cost(self.seed)
        }
    }

    /// Units of the `i`-th item of the job.
    fn units_of(&self, i: u64) -> u64 {
        let (lo, hi) = self.upi;
        match self.cost {
            CostUnit::Item => 1,
            _ if lo == hi => lo,
            _ => lo + Rng(i ^ self.noise_seed.rotate_left(32)).next() % (hi - lo + 1),
        }
    }

    /// What a batch of items of these units costs.
    fn priced(&self, units: &[u64]) -> u64 {
        match self.cost {
            CostUnit::Token => units.iter().max().copied().unwrap_or(0) * units.len() as u64,
            CostUnit::Pixel => units.iter().sum(),
            _ => units.len() as u64,
        }
    }

    /// Items the queue holds for the next window.
    fn queue_hold(&self, window: usize, ran: &[u64], items_left: Option<u64>) -> u64 {
        let mut items = match self.queue.as_str() {
            "full" => DEEP_QUEUE,
            "first1" if window == 0 => 1,
            "first1" => DEEP_QUEUE,
            n => n.parse().expect("queue"),
        };
        if self.lag > 0 {
            let before = match ran.len() >= self.lag {
                true => ran[ran.len() - self.lag],
                false => 1,
            };
            let units = ((self.qmul * before as f64).ceil() as u64).max(self.queue_floor);
            let per_item = match self.cost {
                CostUnit::Item => 1,
                _ => (self.upi.0 + self.upi.1).div_ceil(2).max(1),
            };
            items = items.min(units.div_ceil(per_item));
        }
        items_left.map_or(items, |left| items.min(left))
    }
}

/// A row as the store's own record path writes it.
fn stored_row(sc: &Scenario, anchor: u64, working: Option<u64>) -> ProfileUpdate {
    let (unit, aggregation) = sc.cost_names();
    ProfileUpdate {
        inference_id: "g/v".to_owned(),
        epoch: 1,
        arch: ARCH.to_owned(),
        gpu_name: "TEST 9000".to_owned(),
        torch: "2.7.1+cu128".to_owned(),
        dtype: "fp16".to_owned(),
        unit,
        aggregation,
        base_mb: sc.base,
        base_method: Some("nvml".to_owned()),
        dtype_method: None,
        slope_mb_per_unit: sc.mb_per_unit,
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
                delta_mb: (sc.mb_per_unit * *u as f64) as u64,
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

/// A store file's rows, in the store's own format.
#[derive(serde::Deserialize)]
struct StoreRows {
    #[serde(default)]
    profile: Vec<CalibrationProfile>,
}

/// The local store's working size (-1: none) and retest wait.
fn stored_working(path: &Path) -> (i64, i64) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (-1, 0);
    };
    let rows: StoreRows = toml::from_str(&text).expect("the store's own format");
    let row = rows.profile.iter().find(|row| row.inference_id == "g/v");
    row.map_or((-1, 0), |row| {
        (
            row.knee_units.map_or(-1, |units| units as i64),
            i64::from(row.knee_retest_after),
        )
    })
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

/// The worker's live clamp: `budget` scaled by what memory can spend against
/// what the grant assumed, above the grant's fixed part; shrink-only.
fn scaled(budget: u64, spendable_mb: u64, grant_mb: u64, fixed_mb: u64) -> u64 {
    if spendable_mb >= grant_mb {
        return budget;
    }
    let per_units = grant_mb.saturating_sub(fixed_mb);
    if per_units == 0 {
        return 1;
    }
    let left = spendable_mb.saturating_sub(fixed_mb);
    ((budget as f64 * left as f64 / per_units as f64 + 0.5) as u64).clamp(1, budget)
}

fn ledger_row(ledger: &Arc<VramLedger>) -> crate::inferio::ledger::health::LedgerWorkerHealth {
    ledger
        .health()
        .into_iter()
        .flat_map(|gpu| gpu.workers)
        .find(|worker| worker.inference_id == "g/v")
        .expect("a resident replica")
}

fn register(
    ledger: &Arc<VramLedger>,
    sc: &Scenario,
    device_mb: u64,
) -> (TelemetryHandle, Admission) {
    let handle = sc.device.worker(sc.base, device_mb);
    let admit =
        || ledger.register_worker("g/v", sc.cost_dimension(), &handle, Some(sc.device.key()));
    let admission = admit()
        .or_else(|| {
            ledger.age_death_verdicts_for_test(Duration::from_secs(3600));
            admit()
        })
        .expect("admitted");
    (handle, admission)
}

/// Counters and per-window columns of one process start.
#[derive(Default)]
struct Start {
    items: u64,
    true_s: f64,
    job_ms: f64,
    ooms: u32,
    deaths: u32,
    trims: u32,
    releases: u32,
    clamps: u32,
    trials: u32,
    flips: u32,
    offsize: u32,
    queue_bound: u32,
    grew_under_pressure: u32,
    pool_peak: u64,
    trial_peak: u64,
    rss_peak: u64,
    ram_booked_peak: u64,
    least_slack: i64,
    over_room: u32,
    first_at_stored: i64,
    ran: Vec<u64>,
    working: Vec<u64>,
    window_ms: Vec<u64>,
    window_items: Vec<u64>,
    pools: Vec<u64>,
    died_at: Vec<u64>,
}

fn run_scenario(line: &str, traces: &Path, out: &mut impl std::io::Write) {
    let sc = Scenario::parse(line, traces);
    let mut trace_rng = Rng(sc.trace_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5151);
    let mut load = |path: &PathBuf| {
        let trace = Trace::load(
            path,
            &mut trace_rng,
            sc.trace_min,
            sc.trace_same_start,
            sc.trace_shared_clock,
        );
        let shared = sc.trace_same_start || sc.trace_shared_clock > 0.0 || sc.trace_noise;
        assert!(
            !shared || trace.is_flat(),
            "tshared, tref and tnoise need one series replayed at every size"
        );
        trace
    };
    let trace1 = sc.trace.as_ref().map(&mut load);
    let trace2 = sc.trace2.as_ref().map(&mut load);
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
        scratch.record(stored_row(&sc, sc.shipped, None));
        scratch.write_pending();
        std::fs::create_dir_all(root.path().join("shipped")).unwrap();
        std::fs::copy(
            root.path().join("scratch/calibration.toml"),
            root.path().join("shipped/x.toml"),
        )
        .unwrap();
    }
    let shipped = (sc.shipped > 0).then(|| root.path().join("shipped"));
    let local = root.path().join("local");
    let open_store = || store_at(&local, shipped.clone(), env.clone());
    if let Some(profile) = &sc.profile {
        gpu_only("prof");
        let parts: Vec<&str> = profile.split(':').collect();
        let mut row = stored_row(
            &sc,
            parts[1].parse().unwrap(),
            parts.get(3).map(|k| k.parse().unwrap()),
        );
        if let (Some(failed), Some(wait)) = (parts.get(5), parts.get(7)) {
            row.knee_trials = TrialCadence {
                failed: failed.parse().unwrap(),
                retest_after: wait.parse().unwrap(),
            };
        }
        let store = open_store();
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
    // Every new process reads the store back from disk.
    let mut store = open_store();
    let mut ledger = sc.device.ledger(device_mb, sc.host_ram, &sc.mode, &store);
    let mut next_item = 0u64;
    for start in 0..sc.starts {
        if sc.restart && start > 0 {
            store = open_store();
            ledger = sc.device.ledger(device_mb, sc.host_ram, &sc.mode, &store);
        }
        let switched = start >= sc.switch_start && sc.switch_start > 0;
        let curve = match (&sc.curve2, switched) {
            (Some(curve), true) => curve,
            _ => &sc.curve,
        };
        let trace = match (&trace2, switched) {
            (Some(t), true) => Some(t),
            _ => trace1.as_ref(),
        };
        // A curve gives the job's true rate; a trace alone does not.
        let true_rate = trace.is_none() || sc.trace_noise;
        let (stored_before, _) = stored_working(&local.join("calibration.toml"));
        let (mut handle, mut admission) = register(&ledger, &sc, device_mb);
        let (mut room, mut host_free, mut late_room) = (sc.room, sc.host_free, None);
        if sc.device == Device::GpuRam {
            ledger.record_free_for_test(cpu::DEVICE_KEY, host_free);
        }
        let mut st = Start {
            least_slack: i64::MAX,
            first_at_stored: -1,
            ..Start::default()
        };
        let (mut pool, mut peak_rss, mut under, mut since_load) = (0u64, sc.base, 0u32, 0usize);
        let mut blind_released = false;
        let mut last_grant: Option<Grant> = None;
        let mut now_pressure = mps::MemoryPressure::Normal;
        let job_start_item = next_item;
        let pool_returned = sc.device == Device::Cpu;
        let free_now = |room: u64, pool: u64| match pool_returned {
            true => room,
            false => room.saturating_sub(pool),
        };
        // A trim reply carries the worker's memory sample after the release.
        let release = |admission: &Admission, handle: &TelemetryHandle, room: u64, pool: u64| {
            let left = if pool_returned { sc.base } else { 0 };
            let source = sc.device.free_source();
            push_memory_with_total(handle, free_now(room, left), left, None, source);
            admission.note_trimmed(TrimReply {
                released_mb: Some(pool),
                release_ms: Some(20.0),
            });
        };
        for w in 0..sc.windows {
            let done = next_item - job_start_item;
            if (sc.items > 0 && done >= sc.items)
                || (sc.secs > 0.0 && st.job_ms >= sc.secs * 1000.0)
            {
                break;
            }
            if let Some((_, r)) = sc.room_schedule.iter().find(|(at, _)| *at == w) {
                match sc.room_late {
                    true => late_room = Some(*r),
                    false => room = *r,
                }
            }
            if let Some((_, p)) = sc.pressure.iter().find(|(at, _)| *at == w) {
                now_pressure = *p;
                ledger.set_memory_pressure_for_test(*p);
            }
            if let Some((_, h)) = sc.host_schedule.iter().find(|(at, _)| *at == w) {
                host_free = *h;
                if sc.device == Device::GpuRam {
                    ledger.record_free_for_test(cpu::DEVICE_KEY, host_free);
                }
            }
            ledger.record_free_for_test(sc.device.key(), free_now(room, pool));
            let items_left = (sc.items > 0).then(|| sc.items - done);
            let hold = sc.queue_hold(w, &st.ran, items_left);
            // The dispatcher's window: within the ledger's window target.
            let (target, mut item_bound) = match sc.fixed {
                Some(units) => (units.saturating_mul(WINDOW_DEPTH_MULTIPLIER), usize::MAX),
                None => (
                    in_flight_target_units(admission.window_target_units(), last_grant.as_ref()),
                    admission.window_item_bound(),
                ),
            };
            if let Some(cap) = sc.cap {
                item_bound = item_bound.min((u64::from(cap) * WINDOW_DEPTH_MULTIPLIER) as usize);
            }
            let mut window: Vec<u64> = Vec::new();
            let mut window_units = 0u64;
            while (window.len() as u64) < hold && window.len() < item_bound {
                let units = sc.units_of(next_item + window.len() as u64);
                if !window.is_empty() && window_units + units > target {
                    break;
                }
                window_units += units;
                window.push(units);
            }
            let queued = (hold - window.len() as u64) as usize;
            let before = (sc.fixed.is_none()).then(|| ledger_row(&ledger));
            let token = match sc.fixed {
                Some(_) => None,
                None => {
                    remaining_work(&admission, items_left);
                    let token = admission
                        .request_grant(window_units, sc.cap, window.len(), queued)
                        .expect("granted");
                    last_grant = Some(*token.grant());
                    Some(token)
                }
            };
            let grant = token.as_ref().map(|t| *t.grant());
            if let Some(r) = late_room.take() {
                room = r;
            }
            let budget = match (&grant, sc.fixed) {
                (Some(grant), _) => grant.unit_budget,
                (None, Some(units)) => units,
                (None, None) => unreachable!(),
            };
            let cap_items = grant
                .as_ref()
                .and_then(|g| g.user_cap_items)
                .map_or(usize::MAX, |cap| cap as usize);
            // The worker releases a pool the grants keep well below; a
            // memory-blind grant counts once per release.
            if let Some(g) = grant.filter(|_| pool > 0 && !pool_returned) {
                let below = match g.mb {
                    0 => !blind_released && pool >= SHRINK_BLIND_SLACK_MB,
                    mb => (mb as f64) < SHRINK_RATIO * pool as f64,
                };
                blind_released &= g.mb == 0;
                under = if below { under + 1 } else { 0 };
                if under >= SHRINK_WINDOWS {
                    (pool, under, blind_released) = (0, 0, g.mb == 0);
                    st.releases += 1;
                }
            }
            if token.is_some() && sc.cost == CostUnit::Token {
                // The worker packs a padded batch largest first.
                window.sort_unstable_by(|a, b| b.cmp(a));
            }
            let level = levels.as_mut().map_or(1.0, |l| l.factor(clock_ms));
            let (items_before, mut oom, mut died) = (st.items, false, sc.die.contains(&w));
            let (mut measurements, mut out_ms, mut largest, mut window_pool) =
                (Vec::new(), 0f64, 0u64, pool);
            let mut at = 0;
            while at < window.len() && !died {
                // Memory this batch may spend: the room, and the pool the
                // worker holds, which no other process can take.
                let avail = match pool_returned {
                    true => room,
                    false => room.max(pool),
                };
                // The live clamp, before every batch.
                let mut live = budget;
                let mut clamped = None;
                if let Some(g) = grant.filter(|g| g.mb > 0) {
                    let spendable = match pool_returned {
                        true => room.saturating_sub(g.ram_reserve_mb),
                        false => avail,
                    };
                    live = scaled(budget, spendable, g.mb, g.fixed_mb);
                    if g.ram_mb > 0 {
                        let spare = host_free.saturating_sub(g.ram_reserve_mb);
                        live = live.min(scaled(budget, spare, g.ram_mb, 0));
                    }
                    if live < budget {
                        st.clamps += 1;
                        clamped = Some(ClampReport {
                            from_units: budget,
                            to_units: live,
                            free_mb: Some(free_now(room, pool)),
                            reason: None,
                        });
                    }
                }
                let mut end = at + 1;
                while end < window.len()
                    && end - at < cap_items
                    && sc.priced(&window[at..=end]) <= live
                {
                    end += 1;
                }
                let units = sc.priced(&window[at..end]);
                let next_over_budget = end < window.len()
                    && units as f64 >= NEXT_OVER_BUDGET_MIN_RATIO * budget as f64
                    && sc.priced(&window[at..=end]) > budget;
                let need = (sc.mb_per_unit * units as f64).round() as u64;
                let rss_need = (sc.rss_per_unit * units as f64).round() as u64;
                st.least_slack = st.least_slack.min(avail as i64 - need as i64);
                // Out of host RAM, or out of memory where the kernel kills.
                if (sc.device == Device::GpuRam && rss_need > host_free)
                    || (pool_returned && need > room)
                {
                    died = true;
                    break;
                }
                if need > avail {
                    oom = true;
                    break;
                }
                if let Some(before) = &before {
                    // Above the working size and not the size a trial runs.
                    let working = before.knee_units.unwrap_or(0);
                    st.offsize +=
                        (working > 0 && units > working && before.trial_units != Some(units))
                            as u32;
                }
                let pool_before = pool;
                match pool_returned {
                    true => window_pool = window_pool.max(need),
                    false => pool = pool.max(need),
                }
                window_pool = window_pool.max(pool);
                st.pool_peak = st.pool_peak.max(window_pool);
                let mut ms = match trace {
                    Some(t) if sc.trace_noise => {
                        let (value, mean) = t.ms_per_unit(units, clock_ms);
                        units as f64 * 1000.0 / curve.rate(w, units) * value / mean
                    }
                    Some(t) => units as f64 * t.ms_per_unit(units, clock_ms).0,
                    None => {
                        units as f64 * 1000.0
                            / (curve.rate(w, units) * rng.noise(sc.noise, sc.gauss))
                    }
                } / level;
                if let Some(t) = trace {
                    // The first batches after a load pay their warm-up.
                    match since_load {
                        0 => ms = ms.max(t.first_ms),
                        n => ms += t.warm_ms.get(n - 1).copied().unwrap_or(0.0),
                    }
                }
                since_load += 1;
                if true_rate {
                    st.true_s += units as f64 / curve.rate(w, units);
                }
                st.items += (end - at) as u64;
                largest = largest.max(units);
                clock_ms += ms;
                out_ms += ms;
                let mut m = BatchMeasurement {
                    items: Some((end - at) as u64),
                    units: Some(units),
                    reserved_before_mb: Some(pool_before),
                    reserved_after_mb: Some(pool),
                    peak_reserved_mb: Some(pool),
                    allocated_before_mb: Some(0),
                    peak_allocated_mb: Some(need),
                    duration_ms: Some(ms),
                    next_over_budget,
                    clamped,
                    free_mb: Some(free_now(room, pool_before)),
                    free_source: Some(sc.device.free_source().to_owned()),
                    ..BatchMeasurement::default()
                };
                match sc.device {
                    Device::GpuRam => {
                        m.peak_rss_mb = Some(RSS_AT_LOAD_MB + rss_need);
                        m.rss_after_mb = Some(RSS_AT_LOAD_MB);
                        st.rss_peak = st.rss_peak.max(rss_need);
                    }
                    // Peak resident set as the pool, the live one as
                    // allocated; what the batch freed goes back at once.
                    Device::Cpu => {
                        m.reserved_before_mb = Some(peak_rss);
                        peak_rss = peak_rss.max(sc.base + need);
                        m.reserved_after_mb = Some(peak_rss);
                        m.peak_reserved_mb = Some(peak_rss);
                        m.allocated_before_mb = Some(sc.base);
                        m.peak_allocated_mb = Some(sc.base + need);
                        m.rss_after_mb = Some(sc.base);
                    }
                    _ => {}
                }
                measurements.push(m);
                at = end;
            }
            next_item += st.items - items_before;
            // The pool holds memory another process asked for.
            st.over_room += (!pool_returned && window_pool > room) as u32;
            let overhead = trace.map_or(sc.overhead_ms, |t| t.overhead_ms(largest.max(1)));
            out_ms += overhead;
            clock_ms += overhead;
            if let Some(token) = token
                .as_ref()
                .filter(|_| sc.overhead_ms > 0.0 || trace.is_some())
            {
                token.age_for_test(out_ms);
            }
            if true_rate {
                st.true_s += sc.overhead_ms / 1000.0;
            }
            st.job_ms += out_ms;
            if let (Some(token), Some(before)) = (token, &before) {
                let grant = grant.expect("a grant");
                handle.lock().unwrap().record_measurements(measurements);
                token.finish(match died {
                    true => WindowOutcome::WorkerDied,
                    false => WindowOutcome::Responded {
                        oom: oom.then_some(ErrorFrameOom::Prose),
                    },
                });
                ledger.age_trim_clocks_for_test(
                    admission.worker_id(),
                    Duration::from_secs_f64(out_ms / 1000.0),
                );
                st.ram_booked_peak = st.ram_booked_peak.max(grant.ram_mb);
                st.queue_bound += (window_units < grant.unit_budget) as u32;
                let prev = st.ran.last().copied().unwrap_or(0);
                st.grew_under_pressure += (now_pressure != mps::MemoryPressure::Normal
                    && budget > prev
                    && prev > 0) as u32;
                let after = ledger_row(&ledger);
                let (w0, w1) = (
                    before.knee_units.unwrap_or(0),
                    after.knee_units.unwrap_or(0),
                );
                st.trials += (before.trial_units.is_none() && after.trial_units.is_some()) as u32;
                st.flips += (w0 > 0 && w1 != w0) as u32;
                if before.trial_units.is_some() || after.trial_units.is_some() {
                    st.trial_peak = st.trial_peak.max(window_pool);
                }
                if died {
                    st.deaths += 1;
                    st.died_at.push(w as u64);
                    drop(admission);
                    (handle, admission) = register(&ledger, &sc, device_mb);
                    (pool, since_load, under, last_grant) = (0, 0, 0, None);
                } else if admission.take_trial_trim() {
                    release(&admission, &handle, room, pool);
                    pool = 0;
                    st.trims += 1;
                }
                // The queue runs dry only when the job has nothing left.
                let behind = items_left.map_or(DEEP_QUEUE, |left| left - (st.items - items_before));
                admission.note_demand(behind as usize);
            }
            st.ooms += oom as u32;
            // The first window at the stored size, or holding the whole queue below it.
            let stored = stored_before.max(0) as u64;
            if st.first_at_stored < 0
                && stored > 0
                && (budget == stored || (window_units < stored && budget >= window_units))
            {
                st.first_at_stored = w as i64;
            }
            st.ran.push(budget);
            st.window_ms.push(out_ms.round() as u64);
            st.window_items.push(st.items - items_before);
            st.pools.push(window_pool);
            st.working.push(match sc.fixed {
                Some(units) => units,
                None => ledger_row(&ledger).knee_units.unwrap_or(0),
            });
        }
        let pool_last = pool;
        // The job is over: the dispatcher tells the ledger the queue is dry.
        admission.note_demand(0);
        if admission.take_trial_trim() {
            release(&admission, &handle, room, pool);
            pool = 0;
            st.trims += 1;
        }
        store.write_pending();
        let row = ledger_row(&ledger);
        let (stored, wait) = stored_working(&local.join("calibration.toml"));
        let pool_ms: f64 = st
            .pools
            .iter()
            .zip(&st.window_ms)
            .map(|(p, ms)| (*p * *ms) as f64)
            .sum();
        let opening = st.working.iter().copied().find(|w| *w > 0).unwrap_or(0);
        let working_end = match sc.fixed {
            Some(units) => units as i64,
            None => row.knee_units.map_or(-1, |k| k as i64),
        };
        // `ms`: the job's wall time; `true_s`: its time at the curve's own
        // rates (0 for a trace alone).
        let mut line = format!(
            "name={} start={start} mode={} windows={} items={} ms={:.0} true_s={:.3} W={working_end} \
             open={opening} ostored={stored_before} firstw={} stored={stored} wait={wait} trial={} \
             trials={} flips={} poolmean={:.0} poolpeak={} trialpeak={} poollast={pool_last} \
             poolend={pool} rsspeak={} rssbook={} slack={} oom={} deaths={} trims={} releases={} \
             clamps={} offsize={} qbound={} grewp={} overroom={}",
            sc.name,
            sc.mode,
            st.ran.len(),
            st.items,
            st.job_ms,
            st.true_s,
            st.first_at_stored,
            row.trial_units.map_or(-1, |k| k as i64),
            st.trials,
            st.flips,
            pool_ms / st.job_ms.max(1.0),
            st.pool_peak,
            st.trial_peak,
            st.rss_peak,
            st.ram_booked_peak,
            st.least_slack.min(sc.room as i64),
            st.ooms,
            st.deaths,
            st.trims,
            st.releases,
            st.clamps,
            st.offsize,
            st.queue_bound,
            st.grew_under_pressure,
            st.over_room,
        );
        if sc.verbose {
            let join = |v: &[u64]| match sc.compact {
                true => run_lengths(v),
                false => v.iter().map(u64::to_string).collect::<Vec<_>>().join(","),
            };
            line += &format!(
                " budgets={} working={} wms={} witems={} pools={} died={}",
                join(&st.ran),
                join(&st.working),
                join(&st.window_ms),
                join(&st.window_items),
                join(&st.pools),
                join(&st.died_at),
            );
        }
        writeln!(out, "{line}").unwrap();
        drop(admission);
    }
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

fn field<'a>(line: &'a str, key: &str) -> &'a str {
    line.split_whitespace()
        .find_map(|t| t.strip_prefix(key)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("{key} in {line}"))
}

/// Every device runs to the end with one line per start; a fixed size holds
/// its size; the CPU device's batches leave no pool behind; a restart reads
/// the working size back from disk.
#[test]
fn every_device_simulates_one_line_per_start() {
    let mut spec = String::new();
    for dev in ["gpu-ram", "gpu", "apu", "mac", "cpu"] {
        spec += &format!(
            "name={dev} dev={dev} curve=knee:1.3:128:8 noise=0.05 win=60 starts=2 levels=0.1:5\n"
        );
    }
    spec += "name=fixed dev=gpu fixed=48 win=20 lag=2 v=1\n";
    spec += "name=cpu-pool dev=cpu room=40000 total=48000 pu=46 curve=geo:1.2:20 win=300 v=1\n";
    let mut out = Vec::new();
    run_spec(&spec, Path::new(""), &mut out);
    let out = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 12, "{out}");
    for line in &lines[..10] {
        assert_eq!(field(line, "windows"), "60", "{line}");
    }
    for pair in lines[..10].chunks(2) {
        assert_eq!(
            field(pair[1], "ostored"),
            field(pair[0], "stored"),
            "{pair:?}"
        );
    }
    assert!(
        field(lines[10], "budgets").split(',').all(|b| b == "48"),
        "{}",
        lines[10]
    );
    let cpu = lines[11];
    let largest = field(cpu, "budgets")
        .split(',')
        .map(|b| b.parse::<u64>().unwrap())
        .max();
    assert!(
        largest.unwrap() > 256,
        "the batch memory is not someone else's: {cpu}"
    );
}
