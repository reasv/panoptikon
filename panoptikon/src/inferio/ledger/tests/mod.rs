//! Tests for the memory ledger, and the fixtures they share.
use super::*;
use crate::inferio::calibration::{CalibrationStore, StoreEnv, StorePaths};
use crate::inferio::worker::{ClampReport, OomClass};
use crate::inferio::worker::{LoadReport, MemorySample, Timestamped, WorkerTelemetry};
use crate::test_utils::install_ask_every_event;

use super::external_memory::{free_source_is_authoritative, refresh_due};
use super::grants::{canvas_log_field, clamp_log_field};
use super::load_reservations::OversizedLoad;
use super::measurements::{
    CEILING_CAUSE_PROFILE, CEILING_CAUSE_RAN_WIDER, CEILING_CAUSE_REPORTED,
    CLAMP_REASON_INDEX_LIMIT, knee_admits_window, ram_cost_per_unit, robust_fit,
    update_shape_ceiling, watermark_gap,
};
use super::oom::{
    OOM_SOURCE_ERROR_FRAME, OOM_SOURCE_MARKER, OOM_SOURCE_MESSAGE_PATTERN, OOM_SOURCE_TYPED,
    OOM_SOURCE_UNCLASSIFIED, OomTrust,
};
use super::ramp::{ramp_still_gains, ring_certifies_reached};
use super::test_hooks::CalibrationState;
use super::throughput_knee::{fit_knee, flat_above, relative_mad};

const GPU: &str = "GPU-aaaa";
/// The profile keyspace every test replica reports: one architecture, so
/// two cards of it share a profile and carry separate budgets.
const ARCH: &str = super::TEST_ARCH;

fn item_cost(seed: u32) -> CostDimension {
    CostDimension {
        unit: CostUnit::Item,
        aggregation: Some(CostAggregation::Count),
        epoch: 1,
        seed_units: Some(seed),
        degraded: false,
        canvas_pixels: None,
        max_tokens: None,
    }
}

/// The store query a replica registered with [`item_cost`] produces, as
/// [`loaded`] keys it.
fn item_query(inference_id: &str) -> ProfileQuery<'_> {
    ProfileQuery {
        inference_id,
        epoch: 1,
        arch: ARCH,
        unit: "item",
        aggregation: "count",
        torch: Some("2.7.1+cu128"),
        dtype: Some("fp16"),
    }
}

/// A telemetry handle carrying a load report, as a replica has when the
/// ledger registers it, including the torch build and dtype of its key.
fn loaded(base_mb: Option<u64>, reserved_at_load: Option<u64>) -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb,
        base_method: base_mb.map(|_| "nvml".to_owned()),
        reserved_at_load_mb: reserved_at_load,
        allocated_at_load_mb: reserved_at_load,
        gpu_uuid: Some(GPU.to_owned()),
        gpu_arch: Some(ARCH.to_owned()),
        torch_version: Some("2.7.1+cu128".to_owned()),
        dtype: Some("fp16".to_owned()),
        ..LoadReport::default()
    }));
    Arc::new(StdMutex::new(telemetry))
}

/// [`loaded`] for a named GPU, so a test can put replicas on two cards.
fn loaded_on(gpu: &str, base_mb: Option<u64>, reserved_at_load: Option<u64>) -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb,
        base_method: base_mb.map(|_| "nvml".to_owned()),
        reserved_at_load_mb: reserved_at_load,
        allocated_at_load_mb: reserved_at_load,
        gpu_uuid: Some(gpu.to_owned()),
        gpu_arch: Some(ARCH.to_owned()),
        torch_version: Some("2.7.1+cu128".to_owned()),
        dtype: Some("fp16".to_owned()),
        ..LoadReport::default()
    }));
    Arc::new(StdMutex::new(telemetry))
}

fn ledger(total_mb: u64, budget: VramBudget) -> Arc<VramLedger> {
    VramLedger::for_test(&[(GPU, "TEST 9000", total_mb)], budget)
}

fn ledger_with(total_mb: u64, budget: VramBudget, profiles: &Arc<FakeProfiles>) -> Arc<VramLedger> {
    VramLedger::for_test_with(
        &[(GPU, "TEST 9000", total_mb)],
        budget,
        Some(Arc::clone(profiles) as Arc<dyn CalibrationProfiles>),
    )
}

/// A calibration store stand-in: fixed answers, recorded questions.
#[derive(Default)]
struct FakeProfiles {
    base: Option<u64>,
    seed: Option<ProfileSeed>,
    /// `(inference_id, epoch, arch, torch, dtype)` per `expected_base_mb`
    /// call — the load-reservation tier, where the key is deliberately
    /// incomplete.
    queries: StdMutex<Vec<RecordedQuery>>,
    updates: StdMutex<Vec<ProfileUpdate>>,
}

/// `(inference_id, epoch, arch, torch, dtype)` as `expected_base_mb` saw it.
type RecordedQuery = (String, u32, String, Option<String>, Option<String>);

impl CalibrationProfiles for FakeProfiles {
    fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
        self.queries.lock().unwrap().push((
            query.inference_id.to_owned(),
            query.epoch,
            query.arch.to_owned(),
            query.torch.map(str::to_owned),
            query.dtype.map(str::to_owned),
        ));
        self.base
    }

    /// The same answer, unrecorded: `queries` is about the key the
    /// reservation tier asks with, and the refusal asks with the same one.
    fn refusable_base_mb(&self, _query: &ProfileQuery<'_>) -> Option<u64> {
        self.base
    }

    fn lookup(&self, _query: &ProfileQuery<'_>) -> Option<ProfileSeed> {
        self.seed.clone()
    }

    fn record(&self, update: ProfileUpdate) {
        self.updates.lock().unwrap().push(update);
    }
}

fn no_margin() -> VramBudget {
    user_margin(0.0)
}

/// A margin the user configured: honoured verbatim and uncapped, unlike
/// `VramBudget::default()`, whose default-fraction reserve is capped at
/// [`DEFAULT_RESERVE_CAP_MB`].
fn user_margin(margin: f64) -> VramBudget {
    VramBudget {
        margin: Some(margin),
        cap_fraction: None,
        knee_max_bucket_dispersion: None,
    }
}

/// A `trim` reply from a worker whose `empty_cache()` handed back `mb`.
/// [`TrimReply::default`] is the other case: a worker off CUDA, or one
/// whose pool it could not measure — both still reply `ok`.
fn released(mb: u64) -> TrimReply {
    TrimReply {
        released_mb: Some(mb),
        release_ms: Some(12.0),
    }
}

/// Push a memory sample (our pool size + the GPU's free reading) the way a predict
/// response does.
fn push_memory(handle: &TelemetryHandle, free_mb: u64, reserved_mb: u64) {
    push_memory_with_total(handle, free_mb, reserved_mb, None, "nvml");
}

fn push_memory_with_total(
    handle: &TelemetryHandle,
    free_mb: u64,
    reserved_mb: u64,
    total_mb: Option<u64>,
    source: &str,
) {
    let mut telemetry = handle.lock().unwrap();
    telemetry.memory = Some(Timestamped::now(MemorySample {
        free_mb: Some(free_mb),
        total_mb,
        free_source: Some(source.to_owned()),
        reserved_mb: Some(reserved_mb),
        allocated_mb: Some(reserved_mb),
        ..MemorySample::default()
    }));
}

/// A batch measurement carrying the pre-batch free reading the worker's
/// defensive clamp takes (the per-batch free reading).
fn measurement_with_free(
    units: u64,
    before: u64,
    peak: u64,
    free_mb: u64,
    free_source: &str,
) -> BatchMeasurement {
    BatchMeasurement {
        free_mb: Some(free_mb),
        free_source: Some(free_source.to_owned()),
        ..measurement(units, before, peak)
    }
}

fn measurement(units: u64, before: u64, peak: u64) -> BatchMeasurement {
    BatchMeasurement {
        items: Some(units),
        units: Some(units),
        reserved_before_mb: Some(before),
        peak_reserved_mb: Some(peak),
        allocated_before_mb: Some(before),
        peak_allocated_mb: Some(peak),
        duration_ms: Some(10.0),
        ..BatchMeasurement::default()
    }
}

fn clean_window(admission: &Admission) {
    admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
}

/// A stored profile carrying a fit and a ratchet anchor. `local` is which
/// **file** it came out of — this machine's own store or a shipped baseline
/// — which is not the same question as which card ran it.
fn seeded_anchor(anchor: u64, local: bool) -> ProfileSeed {
    ProfileSeed {
        base_mb: 1000,
        slope_mb_per_unit: 10.0,
        residual_mb: 0.0,
        samples: 20,
        knee_units: None,
        local,
        fit_is_local: local,
        exact_torch: true,
        max_units_measured: anchor,
        local_samples: if local { 20 } else { 0 },
        knee_clean_windows: 0,
        ring: Vec::new(),
    }
}

/// A clean window that reports one pool-growing batch of `units`, and the unit
/// budget it was granted.
fn measured_window(handle: &TelemetryHandle, admission: &Admission, units: u64) -> u64 {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(units, 0, 10 * units + 100)]);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

fn fit_sample_count(ledger: &VramLedger) -> usize {
    ledger
        .calibration_state("g/a", GPU)
        .map(|state| state.samples.len())
        .unwrap_or(0)
}

const AMD_A: &str = "GPU-BDF-0000:03:00.0";
const AMD_B: &str = "GPU-BDF-0000:0c:00.0";

/// A ROCm worker's load report: **no** `gpu_uuid` (the worker suppresses
/// torch's HIP-rendered one), a PCI address, and torch's own total.
fn rocm_report(bdf: Option<&str>, total_mb: Option<u64>) -> LoadReport {
    LoadReport {
        base_mb: Some(1000),
        base_method: Some("alloc_delta".to_owned()),
        reserved_at_load_mb: Some(0),
        allocated_at_load_mb: Some(0),
        gpu_bdf: bdf.map(str::to_owned),
        gpu_total_mb: total_mb,
        torch_version: Some("2.11.0+rocm7.2".to_owned()),
        dtype: Some("fp16".to_owned()),
        ..LoadReport::default()
    }
}

/// [`rocm_report`] as a telemetry handle, which is what registration takes.
fn loaded_rocm(bdf: Option<&str>, total_mb: Option<u64>) -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(rocm_report(bdf, total_mb)));
    Arc::new(StdMutex::new(telemetry))
}

/// The GPU a replica was admitted under, per `/health`.
fn admitted_gpu(ledger: &Arc<VramLedger>, worker: usize) -> (String, String) {
    let gpus = ledger.health();
    let gpu = gpus
        .iter()
        .find(|gpu| !gpu.workers.is_empty())
        .expect("some GPU holds the replica");
    (
        gpu.gpu_uuid.clone(),
        gpu.workers[worker].inference_id.clone(),
    )
}

/// An NVIDIA inventory row, as nvidia-smi's five columns parse to.
fn nvidia(index: u32, uuid: &str, name: &str, total_mb: u64) -> crate::inferio::gpu::GpuInfo {
    crate::inferio::gpu::GpuInfo {
        index,
        uuid: uuid.to_owned(),
        name: name.to_owned(),
        total_mb,
        compute_cap: Some("12.0".to_owned()),
        bdf: None,
        gfx_target_version: None,
        unified_ram_mb: None,
        vram_carveout_mb: None,
    }
}

/// Collects `(level, message)` for everything logged on this thread. The
/// crate has no other way to assert a *level*, and the escalation to WARN
/// is the whole point of `escalate_first_unpriced`.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<StdMutex<Vec<(tracing::Level, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedLogs {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut message = String::new();
        event.record(&mut MessageField(&mut message));
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), message));
    }
}

struct MessageField<'a>(&'a mut String);

impl tracing::field::Visit for MessageField<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            *self.0 = format!("{value:?}");
        }
    }
}

/// Run `body` with the capture installed on **this thread only**
/// (`with_default`), so tests running in parallel cannot see each other's
/// events, and return what it logged.
fn captured_logs(body: impl FnOnce()) -> Vec<(tracing::Level, String)> {
    use tracing_subscriber::layer::SubscriberExt;
    install_ask_every_event();
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::registry().with(logs.clone());
    tracing::subscriber::with_default(subscriber, body);
    logs.0.lock().unwrap().clone()
}

/// `(max_units_measured, persistable_anchor)` for one (model, GPU): the
/// ratchet's ceiling and the only figure the store ever receives.
fn anchors(ledger: &Arc<VramLedger>, model: &str, gpu: &str) -> (u64, u64) {
    let state = ledger.lock();
    let cal = state
        .calibration
        .get(&(model.to_owned(), gpu.to_owned()))
        .expect("a calibration row");
    (cal.max_units_measured, super::persistable_anchor(cal))
}

/// The `max_units_measured` of the last update the store was handed.
fn stored_anchor(profiles: &Arc<FakeProfiles>) -> u64 {
    profiles
        .updates
        .lock()
        .unwrap()
        .last()
        .expect("a store update")
        .max_units_measured
}

const MPS_GPU: &str = "GPU-MPS";
/// A 128 GiB Mac, in MiB.
const MAC_RAM_MB: u64 = 128 * 1024;

/// The one-GPU unified ledger a Mac gets: the probe's 75 % seed, with the
/// host's RAM recorded as the unified-memory bound.
fn mps_ledger() -> Arc<VramLedger> {
    let ledger = VramLedger::for_test_gpus_probed(
        &[(MPS_GPU, "Apple M3 Max (128 GB)", MAC_RAM_MB / 4 * 3, None)],
        no_margin(),
        None,
        GpuMemoryQuery::Mps {
            key: MPS_GPU.to_owned(),
            ram_mb: MAC_RAM_MB,
        },
    );
    ledger
        .lock()
        .gpus
        .get_mut(MPS_GPU)
        .expect("the GPU")
        .unified_ram_mb = Some(MAC_RAM_MB);
    // Metal's allocator, which `for_test_gpus` cannot assume: its CUDA
    // GPUs share the constructor and keep the CUDA pool-margin ceiling.
    ledger.lock().metal_allocator = true;
    ledger
}

/// An MPS worker's load report: no UUID and no PCI address (there is neither on
/// Apple Silicon), and torch's `recommended_max_memory` as the total.
fn loaded_mps(total_mb: Option<u64>) -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1000),
        base_method: Some("mps".to_owned()),
        reserved_at_load_mb: Some(0),
        allocated_at_load_mb: Some(0),
        gpu_name: Some("Apple M3 Max (128 GB)".to_owned()),
        gpu_total_mb: total_mb,
        torch_version: Some("2.7.1".to_owned()),
        ..LoadReport::default()
    }));
    Arc::new(StdMutex::new(telemetry))
}

fn gpu_total_mb(ledger: &Arc<VramLedger>) -> u64 {
    ledger.health()[0].total_mb
}

/// A Metal memory frame: `free_mb` clipped to the device total, and the
/// unclipped RAM reading beside it ([`RamBasis`]).
fn push_ram(
    handle: &TelemetryHandle,
    total_mb: u64,
    available_mb: u64,
    reserved_mb: u64,
    allocated_mb: u64,
) {
    let mut telemetry = handle.lock().unwrap();
    telemetry.memory = Some(Timestamped::now(MemorySample {
        free_mb: Some(available_mb.min(total_mb)),
        total_mb: Some(total_mb),
        free_source: Some("mps".to_owned()),
        reserved_mb: Some(reserved_mb),
        allocated_mb: Some(allocated_mb),
        ram_total_mb: Some(MAC_RAM_MB),
        ram_available_mb: Some(available_mb),
    }));
}

/// A 64 GiB box as its kernel counts it.
const CPU_RAM_MB: u64 = 64 * 1024 - 700;

/// The ledger a CPU-only host gets, built through `VramLedger::new` over a
/// real CPU inventory, which derives the cap default and the adoption scope.
fn cpu_ledger(budgets: impl Into<VramBudgets>) -> Arc<VramLedger> {
    VramLedger::new(
        &crate::inferio::gpu::GpuInventory::known_cpu(CPU_RAM_MB),
        budgets.into(),
        None,
    )
}

/// A CPU worker's load report: no UUID and no PCI address (there is no GPU),
/// `psutil`'s RAM total as `gpu_total_mb`, and the RSS-derived base.
fn loaded_cpu(total_mb: Option<u64>) -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1000),
        base_method: Some("rss".to_owned()),
        reserved_at_load_mb: Some(0),
        allocated_at_load_mb: Some(0),
        gpu_name: Some("CPU (64 GB)".to_owned()),
        gpu_total_mb: total_mb,
        torch_version: Some("2.7.1".to_owned()),
        ..LoadReport::default()
    }));
    Arc::new(StdMutex::new(telemetry))
}

/// A CPU worker's load report on a host that also has GPUs: it names the
/// device it ran on, and nothing else about it identifies a GPU.
fn loaded_on_cpu(total_mb: Option<u64>) -> TelemetryHandle {
    let mut telemetry = WorkerTelemetry::default();
    telemetry.load = Some(Timestamped::now(LoadReport {
        base_mb: Some(1000),
        base_method: Some("rss".to_owned()),
        reserved_at_load_mb: Some(0),
        allocated_at_load_mb: Some(0),
        gpu_name: Some("CPU (64 GB)".to_owned()),
        gpu_arch: Some("cpu".to_owned()),
        gpu_total_mb: total_mb,
        device_kind: Some("cpu".to_owned()),
        torch_version: Some("2.7.1+cpu".to_owned()),
        ..LoadReport::default()
    }));
    Arc::new(StdMutex::new(telemetry))
}

/// `count` observations of one batch size running at `units_per_sec`.
fn rate(units: u64, units_per_sec: f64, count: usize) -> Vec<ThroughputSample> {
    vec![
        ThroughputSample {
            units,
            units_per_sec,
            occupants: 0,
            seq: 0,
            anchor: 0,
            warmup: false,
            warmup_tail: false,
        };
        count
    ]
}

/// A **warm-pool** batch carrying no allocator reading: it reaches the
/// throughput series and, having nothing to price, never the cost fit.
fn warm_batch(units: u64, units_per_sec: f64) -> BatchMeasurement {
    BatchMeasurement {
        items: Some(units),
        units: Some(units),
        reserved_before_mb: Some(1000),
        peak_reserved_mb: Some(1000),
        duration_ms: Some(units as f64 * 1000.0 / units_per_sec),
        ..BatchMeasurement::default()
    }
}

/// One observation as the ledger recorded it: `(units, units/sec, the
/// ratchet anchor at the time, the replica's window index)`.
type Recorded = (u64, f64, u64, u64);

/// A recorded series as [`fit_knee`] receives it — numbered in order, and
/// with the replica's first window marked warm-up.
fn recorded(series: &[Recorded]) -> Vec<ThroughputSample> {
    series
        .iter()
        .enumerate()
        .map(|(index, (units, rate_, anchor, window))| ThroughputSample {
            units: *units,
            units_per_sec: *rate_,
            occupants: 0,
            seq: index as u64,
            anchor: *anchor,
            warmup: *window == 0,
            warmup_tail: false,
        })
        .collect()
}

/// A ledger whose models are all pre-seeded with a 1 MiB/unit fit, so two
/// replicas can hold overlapping windows without the pre-fit rule handing
/// the whole headroom to the first.
fn priced_ledger(total_mb: u64) -> Arc<VramLedger> {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 1.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: None,
            local: false,
            fit_is_local: false,
            exact_torch: true,
            max_units_measured: 0,
            local_samples: 0,
            knee_clean_windows: 0,
            ring: Vec::new(),
        }),
        ..FakeProfiles::default()
    });
    ledger_with(total_mb, no_margin(), &profiles)
}

/// A collapse the window's memory figures corroborate: the batch grew the
/// pool a GiB past `free_mb`, the free reading the device carried before
/// it ran. [`warm_batch`] holds 1 000 MiB of pool to start with.
fn spilled_past_free(units: u64, units_per_sec: f64, free_mb: u64) -> BatchMeasurement {
    BatchMeasurement {
        throughput_collapse: true,
        peak_reserved_mb: Some(2_000 + free_mb + 1_024),
        ..warm_batch(units, units_per_sec)
    }
}

/// A replica capped by a knee on a wide-open GPU, with an anchor big enough that
/// the knee is the binding constraint.
fn knee_capped(knee: u64) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
    let ledger = ledger(200_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .unwrap();
    push_memory(&handle, 190_000, 1000);
    // One measured window, so the anchor is 64 and the knee has something
    // to cap.
    measured_window(&handle, &admission, 64);
    ledger.set_knee_for_test("g/a", GPU, knee);
    (ledger, handle, admission)
}

/// A measured throughput ladder, read at any batch size: linear in
/// log2(units) between the rungs and flat outside them.
fn ladder_rate(ladder: &[(u64, f64)], units: u64) -> f64 {
    let here = (units.max(1) as f64).log2();
    let first = *ladder.first().expect("a ladder has rungs");
    let last = *ladder.last().expect("a ladder has rungs");
    if here <= (first.0 as f64).log2() {
        return first.1;
    }
    for rungs in ladder.windows(2) {
        let (below, above) = (rungs[0], rungs[1]);
        let (low, high) = ((below.0 as f64).log2(), (above.0 as f64).log2());
        if here <= high {
            return below.1 + (above.1 - below.1) * (here - low) / (high - low);
        }
    }
    last.1
}

/// CLIP on an M3 Max: 125.5 items/s at 16 units against 118.7 at 512, for
/// 11.2x the memory.
const CLIP_M3_MAX: [(u64, f64); 6] = [
    (1, 27.9),
    (8, 113.4),
    (16, 125.5),
    (64, 122.9),
    (256, 119.1),
    (512, 118.7),
];

/// wd-vit on the same host and the same probe: the model whose bottom is
/// nearly flat — 26.7 at 1 unit against 29.9 at 8 — while still climbing.
const WDVIT_M3_MAX: [(u64, f64); 5] = [(1, 26.7), (8, 29.9), (16, 29.9), (64, 29.3), (256, 25.8)];

/// MiniLM on the same host and the same probe, tokens/s: still rising at
/// 256 units, and its *slowest* doubling is worth 1.44x.
const MINILM_M3_MAX: [(u64, f64); 5] = [
    (1, 1524.0),
    (8, 13240.0),
    (16, 19080.0),
    (64, 48784.0),
    (256, 104185.0),
];

/// The same card with a wider seed batch. wd-vit ships `seed_units = 64`,
/// so its ladder starts far above the sizes a job's first windows hold.
fn ramping_from_seed(seed: u32) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
    let ledger = ledger(200_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(seed), &handle, None)
        .expect("registers");
    push_memory(&handle, 190_000, 1000);
    (ledger, handle, admission)
}

/// One clean window as a ramping replica runs it:
/// [`WINDOW_DEPTH_MULTIPLIER`] batches at the granted budget, the first
/// growing the pool (the cost fit) and the rest running warm (the
/// throughput ring). Returns the budget it ran at.
fn ramp_window(handle: &TelemetryHandle, admission: &Admission, ladder: &[(u64, f64)]) -> u64 {
    window_at_the_rate(handle, admission, |units| ladder_rate(ladder, units))
}

/// The same window against any rate curve, including a noisy one.
fn window_at_the_rate(
    handle: &TelemetryHandle,
    admission: &Admission,
    rate_at: impl Fn(u64) -> f64,
) -> u64 {
    queued_window_at_the_rate(handle, admission, u64::MAX, rate_at)
}

/// The same window with only `window_units` of work in the queue behind it:
/// what a job's first windows look like while the scanner is still filling
/// them, and the state the ratchet walk starts from.
fn queued_window_at_the_rate(
    handle: &TelemetryHandle,
    admission: &Admission,
    window_units: u64,
    rate_at: impl Fn(u64) -> f64,
) -> u64 {
    let token = admission
        .request_grant(window_units, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    let rate_ = rate_at(granted);
    let mut batches = vec![BatchMeasurement {
        duration_ms: Some(granted as f64 * 1000.0 / rate_),
        ..measurement(granted, 0, 10 * granted + 100)
    }];
    batches.extend((1..WINDOW_DEPTH_MULTIPLIER).map(|_| warm_batch(granted, rate_)));
    handle.lock().unwrap().record_measurements(batches);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

/// One window as an MPS worker reports it: each batch's `peak_reserved` is
/// the sampler's in-batch maximum, while `reserved_after` shows the pool did
/// not move. Returns the budget it ran at.
fn mps_sampled_window(
    handle: &TelemetryHandle,
    admission: &Admission,
    ladder: &[(u64, f64)],
) -> u64 {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    let rate = ladder_rate(ladder, granted);
    let batches = (0..WINDOW_DEPTH_MULTIPLIER)
        .map(|_| BatchMeasurement {
            duration_ms: Some(granted as f64 * 1000.0 / rate),
            reserved_after_mb: Some(1_000),
            ..measurement(granted, 1_000, 10 * granted + 1_000)
        })
        .collect();
    handle.lock().unwrap().record_measurements(batches);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

mod external_memory;
mod grants;
mod host_ram;
mod load_reservations;
mod memory_fit;
mod oom;
mod profiles;
mod ramp;
mod registration;
mod shape_ceiling;
mod throughput_knee;
mod trims;
mod unified_memory;
