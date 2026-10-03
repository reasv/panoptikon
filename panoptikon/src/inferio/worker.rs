//! Worker-process supervision: the orchestrator side of
//! `docs/inferio-worker-protocol.md` (v2).
//!
//! A [`Worker`] wraps one `python -m inferio_worker` child. Frames are 4-byte
//! little-endian u32 length + one msgpack map over the child's stdin/stdout;
//! stderr lines are forwarded to `tracing` with a per-worker prefix and a
//! bounded tail is kept for error reports. The protocol allows exactly one
//! outstanding request per worker, which is enforced structurally: every
//! request method takes `&mut self`.
//!
//! v2 lifecycle: [`Worker::spawn`] performs the handshake only, which
//! carries the worker's *identity* (impl_class + impl_dirs) — no
//! instantiation, so a spawned worker can be prewarmed ([`Worker::prewarm`]
//! runs the impl's optional `prepare()` classmethod) and parked before it
//! is bound to a concrete model via [`Worker::configure`] (which
//! instantiates `impl_class(**config)`), then loaded. Normal (non-pooled)
//! call sites use [`Worker::spawn_configured`], which chains spawn +
//! configure.
//!
//! Failure semantics (protocol doc, "Lifecycle and timeouts"):
//! - `error` frames are per-request failures; the worker stays alive and the
//!   method returns a [`WorkerError`] (downcastable from the `anyhow` chain).
//! - Framing violations (oversized frame, garbage, id mismatch, unexpected
//!   type), deadline timeouts, and worker exit/EOF are fatal, except the
//!   per-batch `memory` frame for the in-flight request. Every fatal path goes
//!   through [`Worker::record_death`], which kills, reaps and poisons the
//!   worker and logs one [`WorkerDeath`].
//! - Graceful stop is the `unload` → terminate → kill ladder with the
//!   deadlines from [`WorkerDeadlines`]. The child additionally sits under a
//!   kill-on-close Job Object on Windows (with PR_SET_PDEATHSIG plus
//!   process-group SIGKILL filling that role on Unix) and tokio
//!   `kill_on_drop`, so neither a drop path nor gateway death itself can
//!   leak a worker tree.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::env;
use std::fmt;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use rmpv::Value;
use serde_json::Value as JsonValue;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

use super::ledger::{DeathKind, FitSnapshot, Grant};
use super::registry::SpawnSpec;
use super::slot_error::{ERROR_SLOT_KEY, SlotError, Unattempted, slot_error_from_parts};
use crate::process_tree::{
    JobGuard, detach_from_console, die_with_parent, first_oom_victim, kill_process_group,
    kill_process_group_pid, spawn_supervised_tokio,
};

/// Protocol version this orchestrator speaks; workers answering anything
/// else in the handshake are killed.
const PROTOCOL_VERSION: u64 = 2;

/// Handshake capability: this orchestrator reads mid-request `memory` frames
/// (protocol doc, "Per-batch memory frames"). A worker that does not know the
/// key ignores it; not a [`PROTOCOL_VERSION`] bump, which would break every
/// older worker venv over an additive key.
const BATCH_MEMORY_FRAMES_FIELD: &str = "batch_memory_frames";

/// That frame's type: the in-flight request id and a memory sample.
const MEMORY_FRAME_TYPE: &str = "memory";

/// Max frame size (2 GiB; must stay below the u32 length-prefix ceiling).
/// Either side treats a larger declared length as a fatal protocol error.
/// Sized for whole-track audio payloads (a raw f32 mono track at 16 kHz is
/// ~230 MiB/hour); both sides buffer a full frame, so the limit is a memory
/// bound, not a correctness one.
pub(crate) const MAX_FRAME_BYTES: usize = 0x8000_0000;

/// Payload budget for the *inputs* of one predict frame: the frame limit
/// minus headroom for the envelope (type/id/keys) and the msgpack encoding
/// overhead of the inputs themselves, so admission arithmetic done on
/// estimated input sizes can never build a frame `encode_frame` refuses.
pub(crate) const FRAME_INPUT_BYTES_BUDGET: usize = MAX_FRAME_BYTES - 8 * 1024 * 1024;

/// Estimated wire size of one predict input: file bytes dominate; JSON data
/// is bounded by its serialized length (msgpack strings/maps are never
/// larger than the JSON text), plus a small per-input framing allowance.
/// Used for byte-aware batch admission — an estimate, which is why
/// [`FRAME_INPUT_BYTES_BUDGET`] keeps a margin under the hard limit.
pub(crate) fn estimate_input_bytes(input: &WorkerInput) -> usize {
    let data = input
        .data
        .as_ref()
        .map(|value| value.to_string().len())
        .unwrap_or(0);
    let file = input.file.as_ref().map(Vec::len).unwrap_or(0);
    data + file + 64
}

/// Bounds for the per-worker stderr tail ring buffer kept for error reports.
const STDERR_TAIL_MAX_LINES: usize = 50;
const STDERR_TAIL_MAX_BYTES: usize = 8 * 1024;

/// How long to wait for the stderr forwarder task to drain after the child
/// exited (it ends on EOF; this only bounds scheduling latency).
const STDERR_JOIN_GRACE: Duration = Duration::from_secs(1);

/// How long a fatal path waits for the killed child to be reaped.
const FATAL_REAP_GRACE: Duration = Duration::from_secs(5);

/// Deadline for a `trim`: a slow `cudaFree` over a big pool is not a wedge.
const TRIM_DEADLINE: Duration = Duration::from_secs(60);

/// Lifecycle deadlines from the protocol doc ("Lifecycle and timeouts").
/// `predict` deliberately has no deadline in v1: models take arbitrarily
/// long, and cancellation means killing the worker.
#[derive(Debug, Clone, Copy)]
pub struct WorkerDeadlines {
    /// Spawn → handshake response (default 30 s). Also used for `configure`
    /// (instantiation is cheap — weights load in `load`) and for `ping`,
    /// whose whole point is bounded liveness checking.
    pub handshake: Duration,
    /// `load` response deadline; long because it covers heavy dependency
    /// imports plus weight loading (default 600 s). Also used for `prewarm`:
    /// `prepare()` exists precisely to run the slow heavy-dependency imports
    /// early, so it gets the load budget, not the handshake one.
    pub load: Duration,
    /// Graceful stop: `unload` sent → `ok` + process exit (default 10 s).
    pub unload_grace: Duration,
    /// After terminate is issued, how long until the hard kill (default 5 s).
    pub terminate_grace: Duration,
}

impl Default for WorkerDeadlines {
    fn default() -> Self {
        Self {
            handshake: Duration::from_secs(30),
            load: Duration::from_secs(600),
            unload_grace: Duration::from_secs(10),
            terminate_grace: Duration::from_secs(5),
        }
    }
}

/// The spawn log's inference id on the prewarm path.
pub const UNCONFIGURED_WORKER: &str = "<unconfigured>";

/// Everything needed to spawn worker processes: interpreter, impl-class
/// search dirs (sent in the handshake), PYTHONPATH prepends (so the child
/// resolves the `inferio_worker` package in the src/ layout), extra env,
/// and working directory.
#[derive(Debug, Clone)]
pub struct WorkerSpawnConfig {
    pub python: PathBuf,
    /// Absolute dirs searched for impl modules, in order; forwarded verbatim
    /// as handshake `impl_dirs`.
    pub impl_dirs: Vec<PathBuf>,
    /// Entries prepended to the child's `PYTHONPATH` (existing value kept,
    /// joined with the platform separator).
    pub pythonpath: Vec<PathBuf>,
    /// Extra environment applied last (wins over the computed entries).
    pub env: Vec<(String, String)>,
    /// Variables explicitly removed after inheritance.
    pub env_remove: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub deadlines: WorkerDeadlines,
    /// The variable a resolved device pin is written to (`gpu::pin_env_var`).
    pub pin_env_var: &'static str,
}

impl WorkerSpawnConfig {
    /// Adds `PANOPTIKON_UNIFIED_GPU=<PCI address>` for a unified GPU.
    pub fn for_unified_device(&self, bdf: Option<&str>) -> Cow<'_, Self> {
        let Some(bdf) = bdf else {
            return Cow::Borrowed(self);
        };
        let mut cfg = self.clone();
        cfg.env.push((
            super::gpu::UNIFIED_GPU_ENV_VAR.to_owned(),
            bdf.to_ascii_lowercase(),
        ));
        Cow::Owned(cfg)
    }

    /// Adds `INFERIO_DEVICE=cpu` for a replica on the CPU device: it is priced
    /// against RAM, so it must run there. Its malloc settings are the CPU
    /// device's, in place of the host accelerator's.
    pub fn for_cpu_device(&self) -> Self {
        let mut cfg = self.clone();
        cfg.env
            .retain(|(key, _)| !crate::accelerator_env::is_device_env(key));
        cfg.env.extend(crate::accelerator_env::cpu_device_env());
        cfg
    }
}

/// One entry of a `predict` request: JSON-like `data` and/or raw `file`
/// bytes, mirroring Python's `PredictionInput`. Absent fields are msgpack
/// nil on the wire.
#[derive(Debug, Clone, Default)]
pub struct WorkerInput {
    pub data: Option<JsonValue>,
    pub file: Option<Vec<u8>>,
}

/// One entry of a `predict` response: msgpack bin stays bytes (serialized
/// numpy etc.), anything else is converted to JSON — except a map carrying
/// the reserved [`ERROR_SLOT_KEY`], which is a typed per-input failure
/// ([`WorkerOutput::Error`]) rather than a payload.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerOutput {
    Bytes(Vec<u8>),
    Json(JsonValue),
    /// This input failed on its own; the rest of the batch is unaffected.
    Error(SlotError),
}

/// One sample of the worker's device memory (protocol doc, "Memory sensing").
/// Absent means unknown, never zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemorySample {
    pub free_mb: Option<u64>,
    pub total_mb: Option<u64>,
    /// Driver behind `free_mb`/`total_mb`; compare samples only within one.
    pub free_source: Option<String>,
    /// Caching-allocator pool size (`torch.cuda.memory_reserved`).
    pub reserved_mb: Option<u64>,
    /// Live tensor bytes (`torch.cuda.memory_allocated`).
    pub allocated_mb: Option<u64>,
    /// Host RAM a unified device's free reading comes out of (`hw.memsize`);
    /// differs from [`Self::total_mb`] on MPS.
    pub ram_total_mb: Option<u64>,
    /// RAM `available` before [`Self::free_mb`] clips it to the device total.
    pub ram_available_mb: Option<u64>,
}

/// The `load` response's footprint report (protocol doc, `load` `ok` table);
/// `base_mb` is the whole-process device footprint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadReport {
    pub base_mb: Option<u64>,
    pub base_method: Option<String>,
    pub reserved_at_load_mb: Option<u64>,
    /// Live tensor MiB at load, the cost fit's baseline; the pool figure on
    /// RAM and MPS.
    pub allocated_at_load_mb: Option<u64>,
    /// Load precision, part of the profile key; `"unstated"` is a value.
    pub dtype: Option<String>,
    /// Diagnostic only.
    pub dtype_method: Option<String>,
    /// Per-item pixel cap introspected from the loaded impl; the registry wins.
    pub canvas_pixels: Option<u32>,
    /// Per-item token cap (`max_seq_length`), likewise.
    pub max_tokens: Option<u32>,
    /// The GPU CUDA device 0 resolved to: the ledger identity. Absent on ROCm.
    pub gpu_uuid: Option<String>,
    /// Informational; the profile key uses the architecture.
    pub gpu_name: Option<String>,
    /// `sm_120`, `gfx1100`, `apple-m3`, `cpu`: the GPU half of the profile key.
    pub gpu_arch: Option<String>,
    /// PCI address (`dddd:bb:dd.0`), the ROCm ledger identity.
    pub gpu_bdf: Option<String>,
    /// Total VRAM per torch, to cross-check a PCI-address match.
    pub gpu_total_mb: Option<u64>,
    /// `torch.__version__`, part of the profile key.
    pub torch_version: Option<String>,
    /// Where torch put the model (`cpu`, `cuda`, `rocm`, `mps`); decides the
    /// ledger device, over any host guess. `None` without torch.
    pub device_kind: Option<String>,
    /// A CUDA or ROCm worker's resident set at load end: the baseline its
    /// host RAM per unit is measured over.
    pub rss_at_load_mb: Option<u64>,
    pub memory: Option<MemorySample>,
}

/// A batch the worker ran smaller than its grant (memory clamp or an impl's
/// shape ceiling), so the ledger does not mistake it for a window tail.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClampReport {
    pub from_units: u64,
    pub to_units: u64,
    /// The free reading the clamp compared against, when there was one.
    pub free_mb: Option<u64>,
    /// `"index_limit"` for a shape ceiling; absent means the memory clamp.
    pub reason: Option<String>,
}

/// The worker's classification of an out-of-memory failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OomClass {
    /// `"typed_exception"`, `"message_pattern"` or `"marker"`.
    pub source: String,
    pub exception: String,
    /// Corroborates a message-pattern match.
    pub free_mb_at_failure: Option<u64>,
    pub device: String,
}

/// One GPU batch the worker ran, from a `predict` or `error` reply (protocol
/// doc, measurement table).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchMeasurement {
    /// Inputs in the batch, not cost-dimension units.
    pub items: Option<u64>,
    /// Size in cost-dimension units; absent without a grant.
    pub units: Option<u64>,
    pub reserved_before_mb: Option<u64>,
    /// The pool after the batch, which answers "did this batch grow the pool"
    /// ([`Self::peak_reserved_mb`] is an in-batch maximum).
    pub reserved_after_mb: Option<u64>,
    pub peak_reserved_mb: Option<u64>,
    pub allocated_before_mb: Option<u64>,
    pub peak_allocated_mb: Option<u64>,
    /// Wall time of `instance.predict(batch)` only.
    pub duration_ms: Option<f64>,
    /// The batch ran out of memory: a negative sample.
    pub oom: bool,
    /// A pool-growing batch whose throughput collapsed: the negative sample
    /// WDDM's sysmem fallback never raises as an OOM.
    pub throughput_collapse: bool,
    /// Part of the pool was in system RAM after this batch: our pool exceeded
    /// the GPU's used memory. A negative sample; the batch's outputs stand.
    pub spilled: bool,
    /// The next item in packing order would have pushed this batch past its
    /// grant's unit budget, and the batch carries at least half of it: it ran
    /// as full as whole items allow.
    pub next_over_budget: bool,
    /// Present when the batch ran below its grant; excluded from throughput.
    pub clamped: Option<ClampReport>,
    /// Present only when [`Self::oom`] is true.
    pub oom_class: Option<OomClass>,
    /// The free reading the clamp took before this batch, and its driver.
    pub free_mb: Option<u64>,
    pub free_source: Option<String>,
    /// On a unified device, the RAM total [`Self::free_mb`] was clipped from.
    pub ram_total_mb: Option<u64>,
    pub ram_available_mb: Option<u64>,
    /// `num_alloc_retries` delta for this batch; `None` off CUDA.
    pub alloc_retries: Option<u64>,
    /// Pool MiB re-grown after a release, on the first batch after it only;
    /// its `duration_ms` includes the re-grow.
    pub regrow_mb: Option<u64>,
    /// `"trim"` (host request) or `"shrink"` (worker's own rule).
    pub regrow_after: Option<String>,
    /// A CUDA or ROCm worker's resident set: its in-batch maximum, and after
    /// the batch.
    pub peak_rss_mb: Option<u64>,
    pub rss_after_mb: Option<u64>,
}

/// What a `trim` measured: MiB handed back and the `empty_cache()` time. Both
/// absent when nothing was released.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TrimReply {
    pub released_mb: Option<u64>,
    pub release_ms: Option<f64>,
}

impl TrimReply {
    fn parse(payload: &[(Value, Value)]) -> Self {
        Self {
            released_mb: field_u64(payload, "released_mb"),
            release_ms: field_f64(payload, "release_ms"),
        }
    }
}

/// A telemetry reading plus when it was recorded.
#[derive(Debug, Clone, PartialEq)]
pub struct Timestamped<T> {
    pub captured_at: Instant,
    pub value: T,
}

impl<T> Timestamped<T> {
    pub(super) fn now(value: T) -> Self {
        Self {
            captured_at: Instant::now(),
            value,
        }
    }
}

/// One recorded batch measurement with a per-worker sequence number.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchSample {
    /// Strictly increasing and never reused, so ring evictions show as gaps.
    pub seq: u64,
    pub captured_at: Instant,
    pub measurement: BatchMeasurement,
}

/// What a worker has told us about its memory, plus its spawn pin. Shared
/// with the manager, which prices it; the dispatcher owns the [`Worker`].
#[derive(Debug, Clone, Default)]
pub struct WorkerTelemetry {
    /// Resolved device pin, for operators; the ledger keys on the worker's
    /// own report.
    pub gpu: Option<String>,
    pub load: Option<Timestamped<LoadReport>>,
    /// Freshest sample, from whichever response carried one last.
    pub memory: Option<Timestamped<MemorySample>>,
    /// Bounded ring of the most recent measurements, oldest first.
    measurements: VecDeque<BatchSample>,
    recorded: u64,
}

impl WorkerTelemetry {
    /// Several windows' worth of batches.
    pub const RING: usize = 256;

    /// Append one `predict`'s measurements, one entry per batch.
    pub(super) fn record_measurements(&mut self, batches: Vec<BatchMeasurement>) {
        for measurement in batches {
            self.record_measurement_at(measurement, Instant::now());
        }
    }

    /// Every batch of one reply under a single capture time, as a coarse
    /// clock can stamp them.
    #[cfg(test)]
    pub(super) fn record_measurements_stamped_alike(&mut self, batches: Vec<BatchMeasurement>) {
        let captured_at = Instant::now();
        for measurement in batches {
            self.record_measurement_at(measurement, captured_at);
        }
    }

    fn record_measurement_at(&mut self, measurement: BatchMeasurement, captured_at: Instant) {
        self.recorded += 1;
        self.measurements.push_back(BatchSample {
            seq: self.recorded,
            captured_at,
            measurement,
        });
        while self.measurements.len() > Self::RING {
            self.measurements.pop_front();
        }
    }

    /// The retained measurements, oldest first. Non-draining: readers keep
    /// their own watermark.
    pub fn measurements(&self) -> impl DoubleEndedIterator<Item = &BatchSample> {
        self.measurements.iter()
    }

    /// Measurements ever reported, including evicted ones.
    pub fn recorded_measurements(&self) -> u64 {
        self.recorded
    }
}

/// Shared handle to one worker's [`WorkerTelemetry`].
pub type TelemetryHandle = Arc<Mutex<WorkerTelemetry>>;

/// A per-request failure reported by a live worker (`error` frame). The
/// worker remains serviceable after this — do not respawn on it.
#[derive(Debug)]
pub struct WorkerError {
    pub message: String,
    pub traceback: String,
    pub stderr_tail: String,
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "worker error: {}", self.message)?;
        if !self.traceback.is_empty() {
            write!(f, "\nworker traceback:\n{}", self.traceback)?;
        }
        if !self.stderr_tail.is_empty() {
            write!(f, "\nworker stderr tail:\n{}", self.stderr_tail)?;
        }
        Ok(())
    }
}

impl std::error::Error for WorkerError {}

/// One worker process's death, captured once by [`Worker::record_death`] as
/// the child is reaped.
#[derive(Debug, Clone)]
pub struct WorkerDeath {
    /// impl_class before `configure`, inference_id after.
    pub worker: String,
    pub pid: Option<u32>,
    /// The reaped status; `None` if the child was wedged rather than gone.
    pub status: Option<ExitStatus>,
    /// Unix terminating signal; read `Some(9)` together with `attribution`.
    pub signal: Option<i32>,
    pub core_dumped: bool,
    pub attribution: DeathAttribution,
    pub why: String,
    pub stderr_tail: String,
}

/// Whose signal killed this worker, sampled before the gateway signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeathAttribution {
    /// Already exited before we signalled: the status is how it died.
    ReapedBeforeSignal,
    /// Going down but not yet reapable; also an outside death.
    Dying,
    /// Alive when the gateway killed it: the SIGKILL is ours.
    StillRunning,
}

impl DeathAttribution {
    pub fn killed_by_gateway(self) -> bool {
        matches!(self, DeathAttribution::StillRunning)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            DeathAttribution::ReapedBeforeSignal => "reaped_before_signal",
            DeathAttribution::Dying => "dying",
            DeathAttribution::StillRunning => "still_running",
        }
    }
}

impl fmt::Display for DeathAttribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// SIGKILL on Linux and macOS: what an out-of-memory killer sends.
const SIGKILL: i32 = 9;

/// Windows exit statuses of a process that ran out of memory:
/// `STATUS_NO_MEMORY` and `STATUS_COMMITMENT_LIMIT`.
const WINDOWS_OUT_OF_MEMORY_STATUSES: [u32; 2] = [0xC000_0017, 0xC000_012D];

/// `None` when the gateway killed the process; otherwise a memory kill for a
/// SIGKILL or a Windows out-of-memory status, and a crash for anything else.
fn death_kind(
    attribution: DeathAttribution,
    signal: Option<i32>,
    code: Option<i32>,
) -> Option<DeathKind> {
    if attribution.killed_by_gateway() {
        return None;
    }
    let out_of_memory = signal == Some(SIGKILL)
        || code.is_some_and(|code| WINDOWS_OUT_OF_MEMORY_STATUSES.contains(&(code as u32)));
    Some(if out_of_memory {
        DeathKind::MemoryKill
    } else {
        DeathKind::Crash
    })
}

impl WorkerDeath {
    pub fn kind(&self) -> Option<DeathKind> {
        death_kind(
            self.attribution,
            self.signal,
            self.status.and_then(|status| status.code()),
        )
    }

    fn status_text(&self) -> String {
        match self.status {
            Some(status) => status.to_string(),
            None => "still running (kill timed out)".to_owned(),
        }
    }
}

impl fmt::Display for WorkerDeath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "process status: {}", self.status_text())?;
        if !self.stderr_tail.is_empty() {
            write!(f, "; stderr tail:\n{}", self.stderr_tail)?;
        } else {
            write!(f, "; stderr tail: <empty>")?;
        }
        Ok(())
    }
}

/// Terminating signal and core-dump flag; `None`/`false` off Unix.
#[cfg(unix)]
fn signal_of(status: &ExitStatus) -> (Option<i32>, bool) {
    use std::os::unix::process::ExitStatusExt;
    (status.signal(), status.core_dumped())
}

#[cfg(not(unix))]
fn signal_of(_status: &ExitStatus) -> (Option<i32>, bool) {
    (None, false)
}

/// Whether this pid is already a zombie that `waitpid(WNOHANG)` does not
/// report yet (`delay_group_leader`). The state follows the last `)`.
#[cfg(target_os = "linux")]
fn leader_is_unwinding(pid: Option<u32>) -> bool {
    let Some(pid) = pid else {
        return false;
    };
    stat_reads_exited(std::fs::read_to_string(format!("/proc/{pid}/stat")))
}

/// Whether a read of `/proc/<pid>/stat` shows a process that is gone or a
/// zombie. A read that failed for another reason (no descriptor to spare)
/// says nothing about the process.
#[cfg(target_os = "linux")]
fn stat_reads_exited(stat: std::io::Result<String>) -> bool {
    let stat = match stat {
        Ok(stat) => stat,
        // Gone while we still hold it unreaped: no longer a process.
        Err(err) => return err.kind() == std::io::ErrorKind::NotFound,
    };
    let Some((_, after_comm)) = stat.rsplit_once(')') else {
        return false;
    };
    matches!(after_comm.split_whitespace().next(), Some("Z" | "X" | "x"))
}

#[cfg(not(target_os = "linux"))]
fn leader_is_unwinding(_pid: Option<u32>) -> bool {
    false
}

/// Bounded ring buffer of recent stderr lines, shared with the forwarder
/// task; snapshots are attached to error reports.
#[derive(Default)]
struct StderrTail {
    lines: VecDeque<String>,
    bytes: usize,
}

impl StderrTail {
    fn push(&mut self, line: String) {
        self.bytes += line.len();
        self.lines.push_back(line);
        while self.lines.len() > STDERR_TAIL_MAX_LINES
            || (self.bytes > STDERR_TAIL_MAX_BYTES && self.lines.len() > 1)
        {
            if let Some(dropped) = self.lines.pop_front() {
                self.bytes -= dropped.len();
            }
        }
    }

    fn snapshot(&self) -> String {
        self.lines.iter().cloned().collect::<Vec<_>>().join("\n")
    }
}

/// A supervised inferio worker process. See the module docs for semantics.
pub struct Worker {
    /// Log/error label: the impl_class from spawn until `configure`
    /// succeeds, then the configured inference_id. The stderr forwarder
    /// keeps the spawn-time impl_class prefix for the worker's whole life
    /// (its identity — a pooled worker may serve any model of the family).
    label: String,
    child: Child,
    /// Latched at spawn: the reap clears `Child::id()`.
    pid: Option<u32>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<StderrTail>>,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
    _job_guard: JobGuard,
    telemetry: TelemetryHandle,
    deadlines: WorkerDeadlines,
    /// Request ids are strictly increasing per worker (sanity checking only,
    /// per the protocol doc).
    next_id: u64,
    /// Set while a request frame may be on the wire without its response
    /// consumed. Entering a new request in this state means a request future
    /// was dropped mid-flight: the stream is desynchronized and the worker
    /// must die (kill() is the cancel path).
    in_flight: bool,
    /// Poisoned by any fatal error; every further call fails fast.
    dead: bool,
    /// The worker stopped answering; a subset of `dead`.
    unreachable: bool,
    death: Option<WorkerDeath>,
    /// Test hook: pretend `try_wait` cannot see the exit status. See
    /// `hide_exit_for_test`.
    #[cfg(test)]
    hide_exit_for_test: bool,
}

/// Why a fatal teardown happened. A mid-window death on a unified-memory
/// device is a negative sample for the ledger; a desync is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FatalCause {
    /// EOF, broken pipe, undecodable frame or expired deadline (`predict`
    /// has no deadline, so a timeout never settles a window).
    Unreachable,
    /// We killed a live worker whose stream can no longer be trusted.
    Desync,
    /// The process had exited before the request was sent: it died idle, not
    /// running this request.
    ExitedIdle,
}

/// The child command for one worker, with its environment
/// (docs/inferio-worker-protocol.md, "Environment"). Exactly one visibility
/// variable is written.
fn worker_command(cfg: &WorkerSpawnConfig, device: Option<&str>) -> Result<Command> {
    let mut command = Command::new(&cfg.python);
    command
        .arg("-m")
        .arg("inferio_worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env("INFERIO_WORKER", "1")
        // UTF-8 regardless of the Windows console code page.
        .env("PYTHONIOENCODING", "utf-8")
        // Never valid for a venv interpreter; AppImage launchers export one.
        .env_remove("PYTHONHOME");
    if !cfg.pythonpath.is_empty() {
        let mut entries = cfg.pythonpath.clone();
        if let Some(existing) = env::var_os("PYTHONPATH") {
            entries.extend(env::split_paths(&existing));
        }
        let joined =
            env::join_paths(&entries).context("PYTHONPATH entries contain the path separator")?;
        command.env("PYTHONPATH", joined);
    }
    if let Some(device) = device {
        command.env(cfg.pin_env_var, device);
        // Tells our placement apart from an operator's visibility variable.
        command.env(super::gpu::DEVICE_PIN_MARKER_ENV_VAR, device);
    }
    for (key, value) in &cfg.env {
        command.env(key, value);
    }
    for key in &cfg.env_remove {
        command.env_remove(key);
    }
    if let Some(cwd) = &cfg.cwd {
        command.current_dir(cwd);
    }
    // Ctrl-C reaches the gateway alone; the shutdown ladder stops workers.
    detach_from_console(&mut command);
    // The kernel reaps the worker if the gateway dies without cleanup.
    die_with_parent(&mut command);
    // Out of RAM, the kernel kills the worker, not another process.
    first_oom_victim(&mut command);
    Ok(command)
}

/// Warn when a model's env config sets a device-selection variable: it is
/// applied last and silently outranks the orchestrator's pin.
fn warn_on_visibility_overrides(cfg: &WorkerSpawnConfig, spec: &SpawnSpec, device: Option<&str>) {
    let overrides = colliding_device_variables(cfg, spec);
    if overrides.is_empty() {
        return;
    }
    tracing::warn!(
        variables = overrides.join(", "),
        pin_variable = cfg.pin_env_var,
        pin = device.unwrap_or("(none)"),
        "this worker's env config sets or removes a device-selection variable \
         (a GPU-visibility one, or the INFERIO_DEVICE coherence marker); it is \
         applied after the device pin, so the worker may not end up on the GPU \
         the ledger is pricing it against"
    );
}

/// The device-selection variables this spawn's model configuration touches.
/// [`DEVICE_ENV_VAR`](crate::accelerator_env::DEVICE_ENV_VAR) is read from the
/// model spec alone, since the orchestrator writes it itself on a CPU host.
fn colliding_device_variables(cfg: &WorkerSpawnConfig, spec: &SpawnSpec) -> Vec<&'static str> {
    let touched = |env: &[(String, String)], removed: &[String], var: &str| {
        env.iter().any(|(key, _)| key.eq_ignore_ascii_case(var))
            || removed.iter().any(|key| key.eq_ignore_ascii_case(var))
    };
    let mut overrides: Vec<&'static str> = super::rocm::VISIBILITY_VARS
        .into_iter()
        .filter(|var| touched(&cfg.env, &cfg.env_remove, var))
        .collect();
    let marker = crate::accelerator_env::DEVICE_ENV_VAR;
    if touched(&spec.env, &spec.env_remove, marker) {
        overrides.push(marker);
    }
    overrides
}

impl Worker {
    /// Spawn `python -m inferio_worker` per the protocol's spawn contract
    /// and perform the identity handshake (no instantiation). On failure the
    /// child is killed and the error carries the traceback or stderr tail.
    /// [`Worker::configure`] must follow before `load`/`predict`.
    pub async fn spawn(
        cfg: &WorkerSpawnConfig,
        impl_class: &str,
        device: Option<String>,
    ) -> Result<Worker> {
        Self::spawn_labelled(cfg, None, impl_class, device).await
    }

    /// [`Self::spawn`] with the inference id for the spawn log line.
    async fn spawn_labelled(
        cfg: &WorkerSpawnConfig,
        inference_id: Option<&str>,
        impl_class: &str,
        device: Option<String>,
    ) -> Result<Worker> {
        let command = worker_command(cfg, device.as_deref())?;
        // Via the spawner thread: PR_SET_PDEATHSIG follows the forking thread.
        let mut child = spawn_supervised_tokio(command).await.with_context(|| {
            format!(
                "failed to spawn inferio worker for impl class {impl_class} via {}",
                cfg.python.display()
            )
        })?;
        // Belt and braces on Windows: kill_on_drop only reaches the direct
        // child, the job object reaps the whole tree on any drop path.
        let job_guard = JobGuard::assign_tokio(&child);
        let pid = child.id();
        tracing::debug!(
            worker = %impl_class,
            inference_id = inference_id.unwrap_or(UNCONFIGURED_WORKER),
            pid = ?pid,
            "spawned an inferio worker"
        );
        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = BufReader::new(child.stdout.take().expect("stdout is piped"));
        let stderr = child.stderr.take().expect("stderr is piped");
        let tail = Arc::new(Mutex::new(StderrTail::default()));
        let stderr_task = tokio::spawn(forward_stderr(
            stderr,
            impl_class.to_owned(),
            Arc::clone(&tail),
        ));

        let mut worker = Worker {
            label: impl_class.to_owned(),
            child,
            pid,
            stdin,
            stdout,
            stderr: tail,
            stderr_task: Some(stderr_task),
            _job_guard: job_guard,
            telemetry: Arc::new(Mutex::new(WorkerTelemetry {
                gpu: device.clone(),
                ..WorkerTelemetry::default()
            })),
            deadlines: cfg.deadlines,
            next_id: 1,
            in_flight: false,
            dead: false,
            unreachable: false,
            death: None,
            #[cfg(test)]
            hide_exit_for_test: false,
        };

        let impl_dirs = cfg
            .impl_dirs
            .iter()
            .map(|dir| Value::from(dir.to_string_lossy().into_owned()))
            .collect();
        let fields = vec![
            (
                Value::from("protocol_version"),
                Value::from(PROTOCOL_VERSION),
            ),
            (Value::from("impl_class"), Value::from(impl_class)),
            (Value::from("impl_dirs"), Value::Array(impl_dirs)),
            (Value::from(BATCH_MEMORY_FRAMES_FIELD), Value::from(true)),
        ];
        let deadline = worker.deadlines.handshake;
        let payload = match worker.roundtrip("handshake", fields, Some(deadline)).await {
            Ok(payload) => payload,
            Err(err) => {
                // A handshake `error` frame leaves the child to exit on its
                // own (the harness exits 1); fatal paths already killed it.
                // kill() is safe in both cases and guarantees the reap.
                worker.kill().await;
                return Err(err.context(format!(
                    "inferio worker handshake failed for impl class {impl_class}"
                )));
            }
        };
        let version = map_get(&payload, "protocol_version").and_then(Value::as_u64);
        if version != Some(PROTOCOL_VERSION) {
            return Err(worker
                .fatal(
                    format!(
                        "worker answered handshake with protocol_version {version:?}, expected {PROTOCOL_VERSION}"
                    ),
                    FatalCause::Desync,
                )
                .await);
        }
        Ok(worker)
    }

    /// Convenience for the normal (non-pooled) flow: spawn + handshake by
    /// impl class, then `configure` for the concrete model. On a configure
    /// failure the (still-alive but useless-to-the-caller) worker is killed
    /// and reaped before the error is returned — call sites always get
    /// either a configured worker or no process at all.
    pub async fn spawn_configured(
        cfg: &WorkerSpawnConfig,
        inference_id: &str,
        spec: &SpawnSpec,
        device: Option<String>,
    ) -> Result<Worker> {
        let mut spawn_cfg = cfg.clone();
        spawn_cfg.env.extend(spec.env.clone());
        spawn_cfg.env_remove.extend(spec.env_remove.clone());
        warn_on_visibility_overrides(&spawn_cfg, spec, device.as_deref());
        let mut worker =
            Self::spawn_labelled(&spawn_cfg, Some(inference_id), &spec.impl_class, device)
                .await
                .with_context(|| format!("failed to spawn inferio worker for {inference_id}"))?;
        if let Err(err) = worker.configure(inference_id, &spec.config_kwargs).await {
            worker.kill().await;
            return Err(err);
        }
        Ok(worker)
    }

    /// Send `configure` — bind this worker to a concrete model by
    /// instantiating `impl_class(**config)` in the child — and await `ok`
    /// within the handshake deadline (instantiation is cheap; weights load
    /// in `load`). Exactly once per worker, before `load`. An `error` frame
    /// (bad kwargs, failing `__init__`, double configure) is a per-request
    /// [`WorkerError`]: the worker stays alive and is NOT poisoned. On
    /// success the worker's log/error label becomes the inference_id.
    pub async fn configure(&mut self, inference_id: &str, config_kwargs: &JsonValue) -> Result<()> {
        let deadline = self.deadlines.handshake;
        let fields = vec![
            (Value::from("inference_id"), Value::from(inference_id)),
            (Value::from("config"), json_to_rmpv(config_kwargs)),
        ];
        self.roundtrip("configure", fields, Some(deadline))
            .await
            .map(|_| ())
            .with_context(|| {
                format!(
                    "configure as {inference_id} failed for inferio worker {}",
                    self.label
                )
            })?;
        self.label = inference_id.to_owned();
        Ok(())
    }

    /// Send `prewarm` — run the impl's optional `prepare()` classmethod
    /// (heavy dependency imports, no weights; absent = no-op) — and await
    /// `ok`. Valid only between handshake and configure; idempotent. Uses
    /// the LOAD deadline, not the handshake one: `prepare()` exists to run
    /// the slow imports early, so it gets the same budget `load` would have
    /// paid (see [`WorkerDeadlines::load`]). An `error` frame is a
    /// per-request [`WorkerError`] and NON-fatal — the worker stays alive
    /// and fully usable (a failed prepare just means load pays the
    /// imports).
    pub async fn prewarm(&mut self) -> Result<()> {
        let deadline = self.deadlines.load;
        self.roundtrip("prewarm", Vec::new(), Some(deadline))
            .await
            .map(|_| ())
            .with_context(|| format!("prewarm failed for inferio worker {}", self.label))
    }

    /// Send `load` and await `ok` within the load deadline. Requires a
    /// prior successful `configure`. Idempotent on the worker side (the
    /// impl's own load() guard).
    /// Records the reported footprint and memory sample in the telemetry.
    pub async fn load(&mut self) -> Result<()> {
        let deadline = self.deadlines.load;
        let payload = self
            .roundtrip("load", Vec::new(), Some(deadline))
            .await
            .with_context(|| format!("load failed for inferio worker {}", self.label))?;
        if let Some(report) = LoadReport::parse(&payload) {
            tracing::debug!(
                worker = %self.label,
                base_mb = report.base_mb,
                base_method = report.base_method.as_deref(),
                dtype = report.dtype.as_deref(),
                dtype_method = report.dtype_method.as_deref(),
                canvas_pixels = report.canvas_pixels,
                gpu_uuid = report.gpu_uuid.as_deref(),
                gpu_name = report.gpu_name.as_deref(),
                gpu_arch = report.gpu_arch.as_deref(),
                gpu_bdf = report.gpu_bdf.as_deref(),
                gpu_total_mb = report.gpu_total_mb,
                torch = report.torch_version.as_deref(),
                "worker reported its load footprint"
            );
            if let Ok(mut telemetry) = self.telemetry.lock() {
                if let Some(sample) = report.memory.clone() {
                    telemetry.memory = Some(Timestamped::now(sample));
                }
                telemetry.load = Some(Timestamped::now(report));
            }
        }
        Ok(())
    }

    pub fn telemetry(&self) -> TelemetryHandle {
        Arc::clone(&self.telemetry)
    }

    /// Record a `predict` reply's memory fields, for `ok` and `error` frames.
    fn record_telemetry(&self, payload: &[(Value, Value)]) {
        let measurements = BatchMeasurement::parse_list(map_get(payload, "measurements"));
        let sample = MemorySample::parse(map_get(payload, "memory"));
        if sample.is_none() && measurements.is_empty() {
            return;
        }
        if let Ok(mut telemetry) = self.telemetry.lock() {
            // Measurements first: the free reading is freshest-wins.
            telemetry.record_measurements(measurements);
            if let Some(sample) = sample {
                telemetry.memory = Some(Timestamped::now(sample));
            }
        }
    }

    /// Send `predict` with the given inputs and return one output per input,
    /// in order. No deadline in v1 (models take arbitrarily long); to cancel,
    /// drop the future and `kill()` the worker.
    ///
    /// With a `grant` the worker splits the inputs into GPU batches and
    /// measures each (protocol doc, "Memory grants").
    ///
    /// A slot may come back as [`WorkerOutput::Error`] — the worker's typed
    /// verdict on that input alone — on an otherwise normal roundtrip.
    pub async fn predict(
        &mut self,
        inputs: &[WorkerInput],
        grant: Option<&Grant>,
        fit: Option<&FitSnapshot>,
    ) -> Result<Vec<WorkerOutput>> {
        let entries = inputs
            .iter()
            .map(|input| {
                Value::Map(vec![
                    (
                        Value::from("data"),
                        input.data.as_ref().map(json_to_rmpv).unwrap_or(Value::Nil),
                    ),
                    (
                        Value::from("file"),
                        input
                            .file
                            .as_ref()
                            .map(|bytes| Value::Binary(bytes.clone()))
                            .unwrap_or(Value::Nil),
                    ),
                ])
            })
            .collect();
        let mut fields = vec![(Value::from("inputs"), Value::Array(entries))];
        if let Some(grant) = grant {
            fields.push((Value::from("grant"), encode_grant(grant)));
        }
        if let Some(fit) = fit {
            fields.push((Value::from("fit"), encode_fit(fit)));
        }
        let mut payload = self
            .roundtrip("predict", fields, None)
            .await
            .with_context(|| format!("predict failed for inferio worker {}", self.label))?;
        let outputs = match take_field(&mut payload, "outputs") {
            Some(Value::Array(outputs)) => outputs,
            other => {
                return Err(self
                    .fatal(
                        format!("predict ok frame without a valid outputs array: {other:?}"),
                        FatalCause::Desync,
                    )
                    .await);
            }
        };
        self.record_telemetry(&payload);
        // A count mismatch would silently mis-route outputs once the
        // dispatcher splits batches per request; the worker cannot be
        // trusted after it.
        if outputs.len() != inputs.len() {
            return Err(self
                .fatal(
                    format!(
                        "worker returned {} outputs for {} inputs",
                        outputs.len(),
                        inputs.len()
                    ),
                    FatalCause::Desync,
                )
                .await);
        }
        let mut converted = Vec::with_capacity(outputs.len());
        for (index, output) in outputs.into_iter().enumerate() {
            match error_slot_from_rmpv(&output) {
                Some(Ok(error)) => converted.push(WorkerOutput::Error(error)),
                // Fatal: guessing a class would let a broken worker fabricate a
                // verdict the store would then persist.
                Some(Err(reason)) => {
                    return Err(self
                        .fatal(
                            format!("predict output {index} is a malformed error slot: {reason}"),
                            FatalCause::Desync,
                        )
                        .await);
                }
                None => match output {
                    Value::Binary(bytes) => converted.push(WorkerOutput::Bytes(bytes)),
                    other => match rmpv_to_json(&other) {
                        Ok(value) => converted.push(WorkerOutput::Json(value)),
                        // The stream is still in sync: a per-request failure.
                        Err(err) => {
                            return Err(anyhow::Error::new(WorkerError {
                                message: format!(
                                    "predict output {index} is not representable as JSON: {err:#}"
                                ),
                                traceback: String::new(),
                                stderr_tail: self.stderr_tail_snapshot(),
                            }));
                        }
                    },
                },
            }
        }
        Ok(converted)
    }

    /// Send `trim` (release the allocator's unused pool) and record the fresh
    /// sample. See docs/batch-calibration-design.md "Trim for idle residents".
    pub async fn trim(&mut self) -> Result<TrimReply> {
        let deadline = TRIM_DEADLINE;
        let payload = self
            .roundtrip("trim", Vec::new(), Some(deadline))
            .await
            .with_context(|| format!("trim failed for inferio worker {}", self.label))?;
        self.record_telemetry(&payload);
        Ok(TrimReply::parse(&payload))
    }

    /// Liveness check: send `ping`, await `ok`. Bounded by the handshake
    /// deadline (an unbounded liveness probe would be useless). The prewarm
    /// pool pings a parked worker before claiming it (protocol doc: it may
    /// have died while parked).
    pub async fn ping(&mut self) -> Result<()> {
        let deadline = self.deadlines.handshake;
        self.roundtrip("ping", Vec::new(), Some(deadline))
            .await
            .map(|_| ())
            .with_context(|| format!("ping failed for inferio worker {}", self.label))
    }

    /// Graceful stop ladder: `unload` → await `ok` + process exit within
    /// `unload_grace`, else terminate, wait `terminate_grace`, then kill.
    /// The child is always reaped. Returns the exit status on the graceful
    /// path (the harness exits 0 after unload).
    pub async fn shutdown(mut self) -> Result<ExitStatus> {
        let name = self.label.clone();
        if self.dead {
            self.kill().await;
            bail!("inferio worker {name} had already failed fatally before shutdown");
        }
        if self.in_flight {
            // A dropped request future left the stream desynchronized; a
            // graceful unload exchange is impossible.
            self.kill().await;
            bail!(
                "inferio worker {name} had a dropped in-flight request; killed instead of graceful unload"
            );
        }
        let id = self.next_id;
        self.next_id += 1;
        let frame = Value::Map(vec![
            (Value::from("type"), Value::from("unload")),
            (Value::from("id"), Value::from(id)),
        ]);
        let bytes = match encode_frame(&frame) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.kill().await;
                return Err(err);
            }
        };
        let grace = self.deadlines.unload_grace;
        let stdin = &mut self.stdin;
        let stdout = &mut self.stdout;
        let child = &mut self.child;
        let graceful = async {
            send_bytes(stdin, &bytes).await?;
            let value = read_frame(stdout).await?;
            let map = match value {
                Value::Map(map) => map,
                other => bail!("unload response is not a map: {other}"),
            };
            let resp_type = map_get(&map, "type").and_then(Value::as_str).unwrap_or("");
            let resp_id = map_get(&map, "id").and_then(Value::as_u64);
            if resp_type != "ok" || resp_id != Some(id) {
                let message = map_get(&map, "message")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                bail!(
                    "unload was not acknowledged (type {resp_type:?}, id {resp_id:?}): {message}"
                );
            }
            let status = child
                .wait()
                .await
                .context("waiting for worker exit after unload")?;
            anyhow::Ok(status)
        };
        let outcome = match timeout(grace, graceful).await {
            Ok(result) => result,
            Err(_) => Err(anyhow!(
                "worker did not acknowledge unload and exit within {grace:?}"
            )),
        };
        match outcome {
            Ok(status) => {
                self.drain_stderr().await;
                Ok(status)
            }
            Err(err) => {
                let death = self
                    .record_death(
                        format!("graceful shutdown failed: {err:#}"),
                        FatalCause::Desync,
                    )
                    .await;
                self.kill().await;
                Err(err.context(format!(
                    "graceful shutdown of inferio worker {name} failed; worker killed; {death}"
                )))
            }
        }
    }

    /// Hard stop: terminate, wait `terminate_grace`, kill again if needed,
    /// and reap. Never fails; also the cancel path for in-flight predicts.
    /// Logs at INFO first unless the worker is already poisoned.
    pub async fn kill(mut self) {
        if !self.dead {
            tracing::info!(
                worker = %self.label,
                pid = ?self.pid,
                "the gateway is stopping this inferio worker (terminate + kill ladder); \
                 a SIGKILL on this pid is ours, not the kernel's"
            );
        }
        // Group first: after the reap the group kill would be a no-op.
        kill_process_group(&self.child);
        let _ = self.child.start_kill();
        if timeout(self.deadlines.terminate_grace, self.child.wait())
            .await
            .is_err()
        {
            // kill() = terminate + wait; the job object / kill_on_drop are
            // the last resort if even this hangs.
            let _ = self.child.kill().await;
        }
        self.drain_stderr().await;
    }

    /// Join the stderr forwarder (bounded by [`STDERR_JOIN_GRACE`]) so the
    /// tail is complete.
    async fn drain_stderr(&mut self) {
        if let Some(task) = self.stderr_task.take() {
            let _ = timeout(STDERR_JOIN_GRACE, task).await;
        }
    }

    /// One request/response cycle: write the frame, read the response,
    /// sanity-check the echoed id, and split ok/error/fatal per the module
    /// docs. `deadline` covers the whole cycle.
    async fn roundtrip(
        &mut self,
        request_type: &str,
        mut fields: Vec<(Value, Value)>,
        deadline: Option<Duration>,
    ) -> Result<Vec<(Value, Value)>> {
        if self.dead {
            return Err(Unattempted::error(format!(
                "inferio worker {} is dead after a previous fatal error",
                self.label
            )));
        }
        if self.in_flight {
            // `Desync` even if the process is gone: missing a negative sample
            // is better than inventing one.
            return Err(self
                .fatal_request(
                    request_type,
                    "a previous request future was dropped mid-flight; the stream is desynchronized"
                        .to_owned(),
                    FatalCause::Desync,
                )
                .await);
        }
        // Not `Unreachable`: this request did not kill it.
        if (!self.exit_hidden() && matches!(self.child.try_wait(), Ok(Some(_))))
            || leader_is_unwinding(self.pid)
        {
            let why = format!("the worker process had exited before this {request_type} request");
            return Err(self.fatal(why, FatalCause::ExitedIdle).await);
        }
        let id = self.next_id;
        self.next_id += 1;
        let mut frame = vec![
            (Value::from("type"), Value::from(request_type)),
            (Value::from("id"), Value::from(id)),
        ];
        frame.append(&mut fields);
        // Serialize fully before sending: an over-limit or unencodable frame
        // fails without a byte hitting the stream, so only this request fails.
        let bytes = match encode_frame(&Value::Map(frame)) {
            Ok(bytes) => bytes,
            Err(err) => {
                return Err(anyhow::Error::new(WorkerError {
                    message: format!("request refused before send: {err:#}"),
                    traceback: String::new(),
                    stderr_tail: String::new(),
                }));
            }
        };

        self.in_flight = true;
        let stdin = &mut self.stdin;
        let stdout = &mut self.stdout;
        let telemetry = &self.telemetry;
        let cycle = async {
            send_bytes(stdin, &bytes).await?;
            // Skip this request's per-batch `memory` frames; any other frame
            // falls through to the checks below. The deadline covers the loop.
            loop {
                let frame = read_frame(stdout).await?;
                if !is_batch_memory_frame(&frame, id) {
                    return Ok(frame);
                }
                record_memory_frame(telemetry, &frame);
            }
        };
        let outcome = match deadline {
            Some(limit) => match timeout(limit, cycle).await {
                Ok(result) => result,
                Err(_) => Err(anyhow!("no response within {limit:?}")),
            },
            None => cycle.await,
        };
        let value = match outcome {
            Ok(value) => value,
            Err(err) => {
                return Err(self
                    .fatal_request(
                        request_type,
                        format!("{request_type} request failed: {err:#}"),
                        FatalCause::Unreachable,
                    )
                    .await);
            }
        };
        let map = match value {
            Value::Map(map) => map,
            other => {
                return Err(self
                    .fatal_request(
                        request_type,
                        format!("response frame is not a map: {other}"),
                        FatalCause::Desync,
                    )
                    .await);
            }
        };
        let resp_type = map_get(&map, "type")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let resp_id = map_get(&map, "id").and_then(Value::as_u64);
        if resp_id != Some(id) {
            return Err(self
                .fatal_request(
                    request_type,
                    format!("response id {resp_id:?} does not match request id {id}"),
                    FatalCause::Desync,
                )
                .await);
        }
        match resp_type.as_deref() {
            Some("ok") => {
                self.in_flight = false;
                Ok(map)
            }
            Some("error") => {
                // The request failed but the exchange completed: the stream
                // is still in sync and the worker stays alive (protocol doc,
                // `error` semantics).
                self.in_flight = false;
                self.record_telemetry(&map);
                let message = map_get(&map, "message")
                    .and_then(Value::as_str)
                    .unwrap_or("<worker sent an error frame without a message>")
                    .to_owned();
                let traceback = map_get(&map, "traceback")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                Err(anyhow::Error::new(WorkerError {
                    message,
                    traceback,
                    stderr_tail: self.stderr_tail_snapshot(),
                }))
            }
            other => Err(self
                .fatal_request(
                    request_type,
                    format!("unexpected response frame type {other:?}"),
                    FatalCause::Desync,
                )
                .await),
        }
    }

    /// [`Self::fatal`], naming a `trim` as the cause when it was one.
    async fn fatal_request(
        &mut self,
        request_type: &str,
        why: String,
        cause: FatalCause,
    ) -> anyhow::Error {
        if request_type == "trim" {
            tracing::warn!(
                worker = %self.label,
                "an idle-resident trim (allocator-pool hygiene, not work) failed \
                 fatally and is about to take this worker down with it; the model \
                 will have to be reloaded. Cause: {why}"
            );
        }
        self.fatal(why, cause).await
    }

    /// Poison the worker via [`Self::record_death`] and return the error.
    async fn fatal(&mut self, why: String, cause: FatalCause) -> anyhow::Error {
        let death = self.record_death(why, cause).await;
        Unattempted::error(format!(
            "inferio worker {} failed fatally: {}; {death}",
            death.worker, death.why
        ))
    }

    /// Sampled before the fatal path signals anything.
    async fn attribute_death(&mut self) -> DeathAttribution {
        if !self.exit_hidden() && matches!(self.child.try_wait(), Ok(Some(_))) {
            return DeathAttribution::ReapedBeforeSignal;
        }
        if self.stdout_at_eof().await || leader_is_unwinding(self.pid) {
            return DeathAttribution::Dying;
        }
        DeathAttribution::StillRunning
    }

    /// Whether stdout is already at EOF, without waiting; bytes found stay
    /// buffered.
    async fn stdout_at_eof(&mut self) -> bool {
        match timeout(Duration::ZERO, self.stdout.fill_buf()).await {
            Ok(Ok(buf)) => buf.is_empty(),
            Ok(Err(_)) | Err(_) => false,
        }
    }

    fn exit_hidden(&self) -> bool {
        #[cfg(test)]
        {
            self.hide_exit_for_test
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    /// The one place a worker dies: poison it, kill and reap the child, drain
    /// stderr, and record and log the death.
    async fn record_death(&mut self, why: String, cause: FatalCause) -> WorkerDeath {
        self.dead = true;
        self.unreachable = matches!(cause, FatalCause::Unreachable);
        self.in_flight = false;
        // Before the kill below, which would read as `signal: 9`.
        let mut attribution = self.attribute_death().await;
        let mut status = None;
        if attribution == DeathAttribution::Dying {
            // One that exits within the grace died by itself and the status
            // is its own; one still running after it dies by our kill.
            status = self.reap(FATAL_REAP_GRACE).await;
            if status.is_none() {
                attribution = DeathAttribution::StillRunning;
            }
        }
        // By pid, for children: a group outlives its reaped leader, and its
        // id is not reused while it has members.
        kill_process_group_pid(self.pid);
        let _ = self.child.start_kill();
        if status.is_none() {
            status = self.reap(FATAL_REAP_GRACE).await;
        }
        self.drain_stderr().await;
        let (signal, core_dumped) = status.as_ref().map(signal_of).unwrap_or((None, false));
        let death = WorkerDeath {
            worker: self.label.clone(),
            pid: self.pid,
            status,
            signal,
            core_dumped,
            attribution,
            why,
            stderr_tail: self.stderr_tail_snapshot(),
        };
        tracing::warn!(
            worker = %death.worker,
            pid = ?death.pid,
            status = %death.status_text(),
            signal = ?death.signal,
            core_dumped = death.core_dumped,
            attribution = death.attribution.as_str(),
            killed_by_gateway = death.attribution.killed_by_gateway(),
            kind = death.kind().map_or("gateway", DeathKind::as_str),
            "an inferio worker process is gone. Cause: {}. stderr tail:\n{}",
            death.why,
            death.stderr_tail,
        );
        self.death = Some(death.clone());
        death
    }

    /// The child's exit status, waiting at most `grace`.
    async fn reap(&mut self, grace: Duration) -> Option<ExitStatus> {
        match timeout(grace, self.child.wait()).await {
            Ok(Ok(status)) => Some(status),
            Ok(Err(err)) => {
                tracing::debug!(worker = %self.label, "reaping the dead worker failed: {err}");
                None
            }
            Err(_) => None,
        }
    }

    /// Liveness check for an idle replica: if the child exited, handle the
    /// death. `None` for an already poisoned worker.
    pub(crate) async fn reap_if_exited(&mut self) -> Option<WorkerDeath> {
        if self.dead {
            return None;
        }
        match self.child.try_wait() {
            Ok(Some(_)) => Some(
                self.record_death(
                    "the worker process exited while idle (no request was in flight)".to_owned(),
                    FatalCause::Unreachable,
                )
                .await,
            ),
            Ok(None) => None,
            Err(err) => {
                tracing::debug!(
                    worker = %self.label,
                    "could not check whether the worker is still running: {err}"
                );
                None
            }
        }
    }

    /// The recorded death, if this worker has died. Test hook: production
    /// reads a death through the WARN line or [`Self::reap_if_exited`].
    #[cfg(test)]
    pub(crate) fn last_death(&self) -> Option<&WorkerDeath> {
        self.death.as_ref()
    }

    /// How this worker died with a request in flight; `None` for a kill the
    /// gateway made. Claimed at most once so one death settles at most one
    /// window.
    pub(crate) fn take_death(&mut self) -> Option<DeathKind> {
        if !std::mem::take(&mut self.unreachable) {
            return None;
        }
        self.death.as_ref().and_then(WorkerDeath::kind)
    }

    fn stderr_tail_snapshot(&self) -> String {
        self.stderr
            .lock()
            .map(|tail| tail.snapshot())
            .unwrap_or_default()
    }

    /// Test hook: kill the child out from under the supervisor, simulating an
    /// external/OOM kill.
    #[cfg(test)]
    pub(crate) async fn kill_child_externally_for_test(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    /// Test hook: strand the stream as a dropped request future would.
    #[cfg(test)]
    pub(crate) fn strand_in_flight_for_test(&mut self) {
        self.in_flight = true;
    }

    /// Test hook: make the pre-signal `try_wait` blind, standing in for the
    /// kernel's `delay_group_leader`.
    #[cfg(test)]
    pub(crate) fn hide_exit_for_test(&mut self) {
        self.hide_exit_for_test = true;
    }
}

/// On Unix a dropped Worker's `kill_on_drop` only reaches the direct child;
/// SIGKILL its process group too, so drop-kill paths (e.g. the dispatcher
/// aborting in-flight windows) cannot orphan worker descendants. A no-op
/// after the explicit kill paths — they reap the child, clearing its id.
/// Windows needs no Drop: the worker's job object drops with it.
#[cfg(unix)]
impl Drop for Worker {
    fn drop(&mut self) {
        if self.child.id().is_some() {
            tracing::debug!(
                worker = %self.label,
                pid = ?self.pid,
                "dropping a live inferio worker; its process group is being killed by the gateway"
            );
        }
        kill_process_group(&self.child);
    }
}

/// Cap on one accumulated stderr "line": a \r-only progress stream (tqdm)
/// never emits \n, so an uncapped line read would grow without bound;
/// oversized chunks are flushed as their own log lines instead.
const STDERR_LINE_CAP: u64 = 64 * 1024;

/// Forward worker stderr lines to tracing and the shared tail buffer.
///
/// The forwarder must stay alive for the worker's whole life no matter what
/// bytes arrive: if it exits early the stderr pipe fills, the worker blocks
/// mid-write, and a deadline-less predict hangs forever. Worker stderr is
/// not guaranteed UTF-8 (e.g. cp1252 tracebacks on Windows, raw progress
/// bars), so lines are read as raw bytes and decoded lossily — only EOF
/// (worker exit) or a fatal read error ends the loop.
async fn forward_stderr(stderr: ChildStderr, inference_id: String, tail: Arc<Mutex<StderrTail>>) {
    let mut reader = BufReader::new(stderr);
    let mut buf: Vec<u8> = Vec::new();
    loop {
        buf.clear();
        // `take` caps a single accumulated line at STDERR_LINE_CAP; a chunk
        // that hits the cap without a newline is flushed as its own line.
        let read = (&mut reader)
            .take(STDERR_LINE_CAP)
            .read_until(b'\n', &mut buf)
            .await;
        match read {
            Ok(0) => break, // EOF: the worker exited.
            Ok(_) => {}
            Err(err) => {
                tracing::debug!(worker = %inference_id, "worker stderr read failed: {err}");
                break;
            }
        }
        while buf
            .last()
            .is_some_and(|byte| *byte == b'\n' || *byte == b'\r')
        {
            buf.pop();
        }
        if buf.is_empty() {
            continue;
        }
        let line = String::from_utf8_lossy(&buf).into_owned();
        tracing::info!(worker = %inference_id, "{line}");
        if let Ok(mut tail) = tail.lock() {
            tail.push(line);
        }
    }
}

/// Serialize one frame payload, enforcing [`MAX_FRAME_BYTES`] before any
/// byte is written (a failure here never corrupts the stream).
fn encode_frame(value: &Value) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    rmpv::encode::write_value(&mut payload, value).context("failed to encode frame payload")?;
    if payload.len() > MAX_FRAME_BYTES {
        bail!(
            "refusing to send a {}-byte frame (over the {MAX_FRAME_BYTES}-byte limit)",
            payload.len()
        );
    }
    Ok(payload)
}

/// Write a length-prefixed frame. Any error here is fatal for the caller
/// (bytes may have been partially written).
async fn send_bytes(stdin: &mut ChildStdin, payload: &[u8]) -> Result<()> {
    stdin
        .write_all(&(payload.len() as u32).to_le_bytes())
        .await
        .context("writing frame header to worker stdin")?;
    stdin
        .write_all(payload)
        .await
        .context("writing frame payload to worker stdin")?;
    stdin.flush().await.context("flushing worker stdin")?;
    Ok(())
}

/// Read one length-prefixed msgpack frame. Any error (EOF, oversized
/// declared length, invalid msgpack) is fatal for the caller.
async fn read_frame(stdout: &mut BufReader<ChildStdout>) -> Result<Value> {
    let mut header = [0u8; 4];
    stdout
        .read_exact(&mut header)
        .await
        .context("reading frame header from worker stdout")?;
    let length = u32::from_le_bytes(header) as usize;
    if length > MAX_FRAME_BYTES {
        bail!("worker declared a {length}-byte frame (over the {MAX_FRAME_BYTES}-byte limit)");
    }
    let mut payload = vec![0u8; length];
    stdout
        .read_exact(&mut payload)
        .await
        .context("reading frame payload from worker stdout")?;
    let value = rmpv::decode::read_value(&mut payload.as_slice())
        .context("frame payload is not valid msgpack")?;
    Ok(value)
}

/// Whole-MiB field: integers as sent, floats rounded, negatives unknown.
fn field_u64(map: &[(Value, Value)], key: &str) -> Option<u64> {
    match map_get(map, key)? {
        Value::Integer(int) => int.as_u64(),
        Value::F32(float) => float_to_u64(f64::from(*float)),
        Value::F64(float) => float_to_u64(*float),
        _ => None,
    }
}

fn float_to_u64(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some(value.round() as u64)
}

fn field_f64(map: &[(Value, Value)], key: &str) -> Option<f64> {
    match map_get(map, key)? {
        Value::F32(float) => Some(f64::from(*float)),
        Value::F64(float) => Some(*float),
        Value::Integer(int) => int.as_f64(),
        _ => None,
    }
    .filter(|value| value.is_finite())
}

fn field_string(map: &[(Value, Value)], key: &str) -> Option<String> {
    map_get(map, key)?.as_str().map(str::to_owned)
}

/// Whether this is the per-batch `memory` frame for the in-flight request;
/// one for any other id is a desync.
fn is_batch_memory_frame(frame: &Value, id: u64) -> bool {
    let Value::Map(map) = frame else {
        return false;
    };
    map_get(map, "type").and_then(Value::as_str) == Some(MEMORY_FRAME_TYPE)
        && map_get(map, "id").and_then(Value::as_u64) == Some(id)
}

/// Fold such a frame's sample into the telemetry as the freshest reading. Not
/// a measurement: the cost fit is untouched.
fn record_memory_frame(telemetry: &TelemetryHandle, frame: &Value) {
    let Value::Map(map) = frame else {
        return;
    };
    let Some(sample) = MemorySample::parse(map_get(map, "memory")) else {
        return;
    };
    if let Ok(mut telemetry) = telemetry.lock() {
        telemetry.memory = Some(Timestamped::now(sample));
    }
}

impl MemorySample {
    /// `None` when absent, not a map, or all nil.
    fn parse(value: Option<&Value>) -> Option<Self> {
        let Value::Map(map) = value? else {
            return None;
        };
        let sample = Self {
            free_mb: field_u64(map, "free_mb"),
            total_mb: field_u64(map, "total_mb"),
            free_source: field_string(map, "free_source"),
            reserved_mb: field_u64(map, "reserved_mb"),
            allocated_mb: field_u64(map, "allocated_mb"),
            ram_total_mb: field_u64(map, "ram_total_mb"),
            ram_available_mb: field_u64(map, "ram_available_mb"),
        };
        (sample != Self::default()).then_some(sample)
    }
}

impl LoadReport {
    /// `None` when the response has no memory-sensing fields (no torch).
    pub(super) fn parse(payload: &[(Value, Value)]) -> Option<Self> {
        let report = Self {
            base_mb: field_u64(payload, "base_mb"),
            base_method: field_string(payload, "base_method"),
            reserved_at_load_mb: field_u64(payload, "reserved_at_load_mb"),
            allocated_at_load_mb: field_u64(payload, "allocated_at_load_mb"),
            dtype: field_string(payload, "dtype"),
            dtype_method: field_string(payload, "dtype_method"),
            canvas_pixels: field_u64(payload, "canvas_pixels")
                .and_then(|pixels| u32::try_from(pixels).ok())
                .filter(|pixels| *pixels >= 1),
            max_tokens: field_u64(payload, "max_tokens")
                .and_then(|tokens| u32::try_from(tokens).ok())
                .filter(|tokens| *tokens >= 1),
            gpu_uuid: field_string(payload, "gpu_uuid"),
            gpu_name: field_string(payload, "gpu_name"),
            gpu_arch: field_string(payload, "gpu_arch"),
            gpu_bdf: field_string(payload, "gpu_bdf"),
            gpu_total_mb: field_u64(payload, "gpu_total_mb"),
            torch_version: field_string(payload, "torch_version"),
            device_kind: field_string(payload, "device_kind"),
            rss_at_load_mb: field_u64(payload, "rss_at_load_mb"),
            memory: MemorySample::parse(map_get(payload, "memory")),
        };
        (report != Self::default()).then_some(report)
    }
}

impl BatchMeasurement {
    fn parse_list(value: Option<&Value>) -> Vec<Self> {
        let Some(Value::Array(entries)) = value else {
            return Vec::new();
        };
        entries
            .iter()
            .filter_map(|entry| {
                let Value::Map(map) = entry else {
                    return None;
                };
                Some(Self {
                    items: field_u64(map, "items"),
                    units: field_u64(map, "units"),
                    reserved_before_mb: field_u64(map, "reserved_before_mb"),
                    reserved_after_mb: field_u64(map, "reserved_after_mb"),
                    peak_reserved_mb: field_u64(map, "peak_reserved_mb"),
                    allocated_before_mb: field_u64(map, "allocated_before_mb"),
                    peak_allocated_mb: field_u64(map, "peak_allocated_mb"),
                    duration_ms: field_f64(map, "duration_ms"),
                    oom: field_bool(map, "oom"),
                    throughput_collapse: field_bool(map, "throughput_collapse"),
                    spilled: field_bool(map, "spilled"),
                    next_over_budget: field_bool(map, "next_over_budget"),
                    clamped: ClampReport::parse(map_get(map, "clamped")),
                    oom_class: OomClass::parse(map_get(map, "oom_class")),
                    free_mb: field_u64(map, "free_mb"),
                    free_source: field_string(map, "free_source"),
                    ram_total_mb: field_u64(map, "ram_total_mb"),
                    ram_available_mb: field_u64(map, "ram_available_mb"),
                    alloc_retries: field_u64(map, "alloc_retries"),
                    regrow_mb: field_u64(map, "regrow_mb"),
                    regrow_after: field_string(map, "regrow_after"),
                    peak_rss_mb: field_u64(map, "peak_rss_mb"),
                    rss_after_mb: field_u64(map, "rss_after_mb"),
                })
            })
            .collect()
    }
}

impl ClampReport {
    /// `None` unless both unit counts are present.
    fn parse(value: Option<&Value>) -> Option<Self> {
        let Value::Map(map) = value? else {
            return None;
        };
        Some(Self {
            from_units: field_u64(map, "from_units")?,
            to_units: field_u64(map, "to_units")?,
            free_mb: field_u64(map, "free_mb"),
            reason: field_string(map, "reason").filter(|reason| !reason.is_empty()),
        })
    }
}

impl OomClass {
    /// `None` when the map is absent or carries no `source`.
    fn parse(value: Option<&Value>) -> Option<Self> {
        let Value::Map(map) = value? else {
            return None;
        };
        Some(Self {
            source: field_string(map, "source")?,
            exception: field_string(map, "exception").unwrap_or_default(),
            free_mb_at_failure: field_u64(map, "free_mb_at_failure"),
            device: field_string(map, "device").unwrap_or_default(),
        })
    }
}

/// Absent or non-boolean reads as `false`.
fn field_bool(map: &[(Value, Value)], key: &str) -> bool {
    matches!(map_get(map, key), Some(Value::Boolean(true)))
}

/// The `grant` map on a `predict` request frame.
fn encode_grant(grant: &Grant) -> Value {
    Value::Map(vec![
        (Value::from("unit_budget"), Value::from(grant.unit_budget)),
        (Value::from("mb"), Value::from(grant.mb)),
        (Value::from("unit"), Value::from(grant.unit.as_str())),
        (
            Value::from("aggregation"),
            Value::from(grant.aggregation.as_str()),
        ),
        (
            Value::from("user_cap_items"),
            grant
                .user_cap_items
                .map(|cap| Value::from(u64::from(cap)))
                .unwrap_or(Value::Nil),
        ),
        // Nil rather than omitted: `None` means "introspect".
        (
            Value::from("canvas_pixels"),
            grant
                .canvas_pixels
                .map(|pixels| Value::from(u64::from(pixels)))
                .unwrap_or(Value::Nil),
        ),
        (
            Value::from("max_tokens"),
            grant
                .max_tokens
                .map(|tokens| Value::from(u64::from(tokens)))
                .unwrap_or(Value::Nil),
        ),
        (Value::from("fixed_mb"), Value::from(grant.fixed_mb)),
        (Value::from("ram_mb"), Value::from(grant.ram_mb)),
        (
            Value::from("ram_reserve_mb"),
            Value::from(grant.ram_reserve_mb),
        ),
    ])
}

/// The `fit` map; sent only when the fitted cost model moved.
fn encode_fit(fit: &FitSnapshot) -> Value {
    Value::Map(vec![
        (
            Value::from("slope_mb_per_unit"),
            Value::F64(fit.slope_mb_per_unit),
        ),
        (Value::from("intercept_mb"), Value::F64(fit.intercept_mb)),
        (Value::from("residual_mb"), Value::F64(fit.residual_mb)),
        (Value::from("samples"), Value::from(fit.samples as u64)),
    ])
}

fn map_get<'a>(map: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .map(|(_, v)| v)
}

/// Reads one output slot as a typed error: `None` for an ordinary payload,
/// `Some(Err(..))` for a malformed error object (a protocol violation).
fn error_slot_from_rmpv(value: &Value) -> Option<Result<SlotError, String>> {
    let Value::Map(entries) = value else {
        return None;
    };
    let body = map_get(entries, ERROR_SLOT_KEY)?;
    let Value::Map(body) = body else {
        return Some(Err(format!("`{ERROR_SLOT_KEY}` is not a map: {body}")));
    };
    Some(slot_error_from_parts(
        map_get(body, "class").and_then(Value::as_str),
        map_get(body, "message").and_then(Value::as_str),
    ))
}

fn take_field(map: &mut Vec<(Value, Value)>, key: &str) -> Option<Value> {
    let index = map.iter().position(|(k, _)| k.as_str() == Some(key))?;
    Some(map.swap_remove(index).1)
}

/// JSON → msgpack value. Straightforward except numbers: serde_json numbers
/// are exactly one of i64/u64/f64, and each maps to the corresponding
/// msgpack representation so ints stay ints end-to-end.
fn json_to_rmpv(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Nil,
        JsonValue::Bool(b) => Value::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::from(i)
            } else if let Some(u) = n.as_u64() {
                Value::from(u)
            } else {
                // as_f64 is total for serde_json numbers that are not ints.
                Value::F64(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        JsonValue::String(s) => Value::from(s.as_str()),
        JsonValue::Array(items) => Value::Array(items.iter().map(json_to_rmpv).collect()),
        JsonValue::Object(map) => Value::Map(
            map.iter()
                .map(|(key, value)| (Value::from(key.as_str()), json_to_rmpv(value)))
                .collect(),
        ),
    }
}

/// msgpack → JSON value for JSON-like predict outputs. Non-string map keys
/// are coerced via their msgpack display form (should not occur — Python
/// dict keys from impls are strings). Binary/ext nested *inside* a JSON-like
/// value has no JSON form and fails the conversion (top-level bin is handled
/// as [`WorkerOutput::Bytes`] before this is called).
fn rmpv_to_json(value: &Value) -> Result<JsonValue> {
    Ok(match value {
        Value::Nil => JsonValue::Null,
        Value::Boolean(b) => JsonValue::Bool(*b),
        Value::Integer(i) => {
            if let Some(v) = i.as_i64() {
                JsonValue::from(v)
            } else if let Some(v) = i.as_u64() {
                JsonValue::from(v)
            } else {
                bail!("msgpack integer {i} fits neither i64 nor u64")
            }
        }
        Value::F32(f) => serde_json::Number::from_f64(f64::from(*f))
            .map(JsonValue::Number)
            .with_context(|| format!("non-finite float {f} has no JSON form"))?,
        Value::F64(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .with_context(|| format!("non-finite float {f} has no JSON form"))?,
        Value::String(s) => JsonValue::String(
            s.as_str()
                .context("worker sent a non-UTF-8 msgpack string")?
                .to_owned(),
        ),
        Value::Binary(_) => bail!("binary data nested inside a JSON-like output has no JSON form"),
        Value::Array(items) => {
            JsonValue::Array(items.iter().map(rmpv_to_json).collect::<Result<_>>()?)
        }
        Value::Map(entries) => {
            let mut map = serde_json::Map::with_capacity(entries.len());
            for (key, value) in entries {
                let key = match key.as_str() {
                    Some(s) => s.to_owned(),
                    None => key.to_string(),
                };
                map.insert(key, rmpv_to_json(value)?);
            }
            JsonValue::Object(map)
        }
        Value::Ext(tag, _) => bail!("msgpack ext type {tag} has no JSON form"),
    })
}

/// Spawn plumbing shared by the worker's own tests and the dispatcher's,
/// which needs real worker subprocesses to drive a granted window end to end.
#[cfg(test)]
pub(super) mod testing {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    /// Repo root = CARGO_MANIFEST_DIR/.. (the panoptikon crate lives one level
    /// below the workspace root).
    pub(crate) fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    /// Test interpreter default: the managed venv (`python/.venv`) if
    /// present, else the legacy root `.venv` (pre-restructure installs).
    pub(crate) fn test_venv_python(root: &Path, rel: &str) -> PathBuf {
        let managed = root.join("python/.venv").join(rel);
        if managed.is_file() {
            managed
        } else {
            root.join(".venv").join(rel)
        }
    }

    /// Spawn config matching how the Python protocol tests drive the harness:
    /// repo venv python, cwd = repo root, PYTHONPATH=python, NO_CUDNN.
    pub(crate) fn test_spawn_config() -> WorkerSpawnConfig {
        let root = workspace_root();
        // PANOPTIKON_TEST_PYTHON overrides the repo-venv interpreter (any
        // python with msgpack works), e.g. running the suite under WSL.
        let python = match std::env::var_os("PANOPTIKON_TEST_PYTHON") {
            Some(explicit) => PathBuf::from(explicit),
            None if cfg!(windows) => test_venv_python(&root, "Scripts/python.exe"),
            None => test_venv_python(&root, "bin/python"),
        };
        if !python.is_file() {
            panic!(
                "inferio worker tests need the repo venv interpreter at {} — create the dev venv first",
                python.display()
            );
        }
        WorkerSpawnConfig {
            python,
            impl_dirs: vec![root.join("python/tests/inferio_worker/fixture_impls")],
            pythonpath: vec![root.join("python")],
            env: vec![("NO_CUDNN".to_owned(), "true".to_owned())],
            env_remove: Vec::new(),
            cwd: Some(root),
            deadlines: WorkerDeadlines::default(),
            // The fixture impls echo `CUDA_VISIBLE_DEVICES`.
            pin_env_var: crate::inferio::gpu::CUDA_PIN_ENV_VAR,
        }
    }

    pub(crate) fn spec(impl_class: &str) -> SpawnSpec {
        SpawnSpec {
            impl_class: impl_class.to_owned(),
            config_kwargs: json!({}),
            device_pins: vec![None],
            env: Vec::new(),
            env_remove: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use serde_json::json;

    /// One JSON-data input — the shape most of these predicts use.
    fn one(data: JsonValue) -> [WorkerInput; 1] {
        [WorkerInput {
            data: Some(data),
            file: None,
        }]
    }

    /// A worker spawned and configured on the named fixture impl.
    async fn configured(id: &str, impl_class: &str) -> Worker {
        Worker::spawn_configured(&test_spawn_config(), id, &spec(impl_class), None)
            .await
            .expect("spawn + handshake")
    }

    /// The same, loaded.
    async fn loaded(id: &str, impl_class: &str) -> Worker {
        let mut worker = configured(id, impl_class).await;
        worker.load().await.expect("load ok");
        worker
    }

    /// An item/count grant of `unit_budget` items per GPU batch.
    fn item_grant(unit_budget: u64) -> Grant {
        Grant {
            unit_budget,
            mb: 1024,
            unit: super::super::cost::CostUnit::Item,
            aggregation: super::super::cost::CostAggregation::Count,
            user_cap_items: None,
            canvas_pixels: None,
            max_tokens: None,
            squeezed: false,
            fixed_mb: 0,
            ram_mb: 0,
            ram_reserve_mb: 0,
        }
    }

    /// A resolved pin goes into the visibility variable the backend dictates
    /// and into no other one, with the unified-GPU address and the placement
    /// marker riding alongside it. Asserted against the composed command, so
    /// it holds with no interpreter and no GPU.
    #[test]
    fn the_device_pin_goes_into_the_backends_visibility_variable() {
        use crate::inferio::gpu::{CUDA_PIN_ENV_VAR, HIP_PIN_ENV_VAR};

        fn config(pin_env_var: &'static str) -> WorkerSpawnConfig {
            WorkerSpawnConfig {
                python: PathBuf::from("python"),
                impl_dirs: Vec::new(),
                pythonpath: Vec::new(),
                env: Vec::new(),
                env_remove: Vec::new(),
                cwd: None,
                deadlines: WorkerDeadlines::default(),
                pin_env_var,
            }
        }
        fn env_of(cfg: &WorkerSpawnConfig, device: Option<&str>, name: &str) -> Option<String> {
            worker_command(cfg, device)
                .expect("the command composes")
                .as_std()
                .get_envs()
                .find(|(key, _)| key.to_string_lossy() == name)
                .map(|(_, value)| value.unwrap_or_default().to_string_lossy().into_owned())
        }
        let cuda = config(CUDA_PIN_ENV_VAR);
        let rocm = config(HIP_PIN_ENV_VAR);
        // (config, pin, the variable that must carry it, the one that must
        // stay unset). CUDA_VISIBLE_DEVICES is a HIP alias and setting both is
        // documented unintended-behaviour territory; a `GPU-…` string in HIP's
        // variable would hide every GPU.
        #[rustfmt::skip]
        let backends = [
            (&cuda, "GPU-1a2b", "CUDA_VISIBLE_DEVICES", "HIP_VISIBLE_DEVICES"),
            (&rocm, "1", "HIP_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES"),
        ];
        for (cfg, pin, written, silent) in backends {
            assert_eq!(env_of(cfg, Some(pin), written).as_deref(), Some(pin));
            assert_eq!(env_of(cfg, Some(pin), silent), None, "{silent} stays unset");
            // The same pin under a name only we write, so the worker can tell
            // our placement from an operator's ambient visibility variable.
            assert_eq!(
                env_of(cfg, Some(pin), "PANOPTIKON_DEVICE_PIN").as_deref(),
                Some(pin)
            );
            // No pin, no variable and no marker: an unpinned replica was
            // placed by nobody, and inherits whatever the operator set.
            assert_eq!(env_of(cfg, None, written), None);
            assert_eq!(env_of(cfg, None, "PANOPTIKON_DEVICE_PIN"), None);
        }

        // A replica on a **unified** GPU is told which GPU that is, as an
        // address rather than a flag: the pin is only a belief about where it
        // lands, and the worker checks the value against the GPU it actually
        // came up on. Lower-cased, the spelling the worker renders its own in;
        // absent, never zero, on a discrete GPU.
        let unified = |cfg: &WorkerSpawnConfig, bdf: Option<&str>, key| {
            env_of(&cfg.for_unified_device(bdf), Some("0"), key)
        };
        let gpu = "PANOPTIKON_UNIFIED_GPU";
        let bdf = Some("0000:03:00.0");
        assert_eq!(unified(&rocm, bdf, gpu).as_deref(), bdf);
        let upper = Some("0000:0C:00.0");
        assert_eq!(
            unified(&rocm, upper, gpu).as_deref(),
            Some("0000:0c:00.0"),
            "lower-cased, the spelling the worker renders its own in"
        );
        assert_eq!(unified(&rocm, None, gpu), None);
        assert_eq!(unified(&cuda, None, gpu), None);
        // The pin itself is untouched by either answer, and a discrete replica
        // spawns with the caller's own config — the flag is the only reason to
        // clone one.
        assert_eq!(
            unified(&rocm, bdf, "HIP_VISIBLE_DEVICES").as_deref(),
            Some("0")
        );
        assert!(matches!(
            rocm.for_unified_device(None),
            std::borrow::Cow::Borrowed(_)
        ));

        // A replica the ledger placed on the **CPU device** of a host that has
        // GPUs: every GPU hidden (the pin `gpu::resolve_pin` answers for a
        // `cpu` request), and the marker `get_device` reads, so the model runs
        // where it is priced.
        for cfg in [&cuda, &rocm] {
            // Spawned from the GPU host's worker env, arena cap included.
            let mut gpu_host = cfg.clone();
            gpu_host.env = crate::accelerator_env::worker_env(
                crate::config::Accelerator::Cuda,
                std::path::Path::new("/nonexistent/venv/bin/python"),
            );
            let on_cpu = gpu_host.for_cpu_device();
            assert_eq!(
                env_of(&on_cpu, Some(""), cfg.pin_env_var).as_deref(),
                Some(""),
                "an empty visibility value is what hides every GPU"
            );
            assert_eq!(
                env_of(&on_cpu, Some(""), crate::accelerator_env::DEVICE_ENV_VAR).as_deref(),
                Some("cpu")
            );
            assert_eq!(
                env_of(cfg, Some(""), crate::accelerator_env::DEVICE_ENV_VAR),
                None,
                "and nothing else gets the marker"
            );
            // Its memory is priced as resident set, like a CPU host's workers:
            // the CPU thresholds, not the GPU workers' arena cap.
            assert_eq!(
                env_of(&on_cpu, Some(""), "MALLOC_MMAP_THRESHOLD_").as_deref(),
                Some("131072")
            );
            assert_eq!(env_of(&on_cpu, Some(""), "MALLOC_ARENA_MAX"), None);
        }
    }

    /// The device-override warning fires on the **model's** configuration and
    /// never on the orchestrator's own `INFERIO_DEVICE`, which it writes on
    /// every worker of a CPU-priced host.
    #[test]
    fn the_device_override_warning_reads_the_models_own_env() {
        use crate::accelerator_env::DEVICE_ENV_VAR;
        use crate::inferio::gpu::CUDA_PIN_ENV_VAR;

        /// A spawn config as `http.rs` builds one, plus the model's entries
        /// merged on top exactly as `spawn_configured` merges them.
        fn collisions(host_env: Vec<(String, String)>, spec: &SpawnSpec) -> Vec<&'static str> {
            let mut cfg = WorkerSpawnConfig {
                python: PathBuf::from("python"),
                impl_dirs: Vec::new(),
                pythonpath: Vec::new(),
                env: host_env,
                env_remove: Vec::new(),
                cwd: None,
                deadlines: WorkerDeadlines::default(),
                pin_env_var: CUDA_PIN_ENV_VAR,
            };
            cfg.env.extend(spec.env.clone());
            cfg.env_remove.extend(spec.env_remove.clone());
            colliding_device_variables(&cfg, spec)
        }
        let cpu_host = || vec![(DEVICE_ENV_VAR.to_owned(), "cpu".to_owned())];
        let configured = |env: Vec<(&str, &str)>, removed: Vec<&str>| {
            let mut spec = spec("echo_test");
            spec.env
                .extend(env.into_iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
            spec.env_remove
                .extend(removed.into_iter().map(str::to_owned));
            spec
        };

        // The orchestrator's own marker sits in the merged env of every
        // CPU-priced host and must not warn about itself. The model setting it
        // is the collision; deleting it is the same one, since the worker then
        // probes and can land off the GPU it is priced against; and it is
        // matched case-insensitively, like every other env comparison here.
        // The visibility arm is read from the merged env, every variant, in
        // `VISIBILITY_VARS` order — and is never the orchestrator's, which
        // writes no visibility variable through `env` at all.
        const CUDA: &str = "CUDA_VISIBLE_DEVICES";
        const ROCR: &str = "ROCR_VISIBLE_DEVICES";
        #[rustfmt::skip]
        let cases = [
            ("nothing configured", configured(vec![], vec![]), vec![]),
            ("the model sets the marker", configured(vec![(DEVICE_ENV_VAR, "cuda")], vec![]), vec![DEVICE_ENV_VAR]),
            ("the model deletes it", configured(vec![], vec![DEVICE_ENV_VAR]), vec![DEVICE_ENV_VAR]),
            ("lower case", configured(vec![("inferio_device", "cuda")], vec![]), vec![DEVICE_ENV_VAR]),
            ("visibility variables", configured(vec![(CUDA, "0")], vec![ROCR]), vec![ROCR, CUDA]),
            ("both families, marker last", configured(vec![(CUDA, "0"), (DEVICE_ENV_VAR, "cuda")], vec![ROCR]), vec![ROCR, CUDA, DEVICE_ENV_VAR]),
        ];
        for (label, spec, expected) in cases {
            assert_eq!(collisions(cpu_host(), &spec), expected, "{label}");
        }
        // And on a host with no marker of its own, the model's entry is still
        // the collision.
        let overriding = configured(vec![(DEVICE_ENV_VAR, "cuda")], vec![]);
        assert_eq!(collisions(Vec::new(), &overriding), vec![DEVICE_ENV_VAR]);
    }

    /// Full happy path against a real worker subprocess, plus data fidelity:
    /// a mixed predict (a JSON value exercising nested unicode, large ints,
    /// floats, bools, null, lists and maps, then raw file bytes) returns
    /// ordered outputs of the right variants with exact serde_json equality,
    /// and shutdown unloads gracefully with the worker exiting 0.
    #[tokio::test]
    async fn full_lifecycle_happy_path() {
        let mut worker = loaded("test/echo", "echo_test").await;

        // Exercises nested unicode, positive/negative/large integers, floats,
        // booleans, null, lists and maps: the JSON → msgpack → Python →
        // msgpack → JSON round trip must be exactly equal (ints stay ints).
        let data = json!({
            "unicode": "こんにちは — ünïcode ✓ emoji 🦀 内",
            "int": 42,
            "negative": -7,
            "big": 9_007_199_254_740_993_i64,
            "float": 3.25,
            "bool": true,
            "null": null,
            "list": [1, "two", 3.5, false, null, {"nested": "map"}],
            "map": {"inner": {"deep": ["リスト", 2.0, -1]}}
        });
        let inputs = [
            one(data.clone())[0].clone(),
            WorkerInput {
                data: None,
                file: Some(vec![0x00, 0x01, 0xfe, 0xff]),
            },
        ];
        let outputs = worker
            .predict(&inputs, None, None)
            .await
            .expect("predict ok");
        assert_eq!(
            outputs,
            vec![
                WorkerOutput::Json(json!({"echo": data})),
                WorkerOutput::Bytes(b"echo:\x00\x01\xfe\xff".to_vec()),
            ],
            "one output per input, in order and of the right variant"
        );

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0), "worker exits 0 after unload");
    }

    /// `trim` round trip over the real protocol: idempotent, and a plain
    /// success on a worker with nothing to release (the fixtures import no
    /// torch), with the following predict proving the stream stayed in sync.
    #[tokio::test]
    async fn a_trim_is_answered_and_leaves_the_worker_serving() {
        let mut worker = loaded("test/echo", "echo_test").await;

        worker.trim().await.expect("trim is answered with ok");
        worker.trim().await.expect("trim is idempotent");

        let outputs = worker
            .predict(&one(json!("still here")), None, None)
            .await
            .expect("the worker still serves predicts after a trim");
        assert_eq!(
            outputs[0],
            WorkerOutput::Json(json!({"echo": "still here"}))
        );
        worker.shutdown().await.expect("graceful shutdown");
    }

    /// A `predict` carrying a **grant** makes the worker's packing harness
    /// split the window into GPU batches inside the unit budget, report one
    /// priced measurement each, and still answer one output per input in
    /// order. The batchsize fixture reports the size it was handed, so the
    /// packing is asserted in the worker rather than inferred from telemetry.
    #[tokio::test]
    async fn a_grant_makes_the_worker_pack_gpu_batches() {
        let mut worker = loaded("test/batch", "batchsize_test").await;
        let telemetry = worker.telemetry();

        let inputs: Vec<WorkerInput> = (0..5).flat_map(|index| one(json!(index))).collect();
        let grant = item_grant(2);
        let sizes = |outputs: &[WorkerOutput]| -> Vec<u64> {
            outputs
                .iter()
                .map(|output| match output {
                    WorkerOutput::Json(value) => value["batch"].as_u64().expect("batch"),
                    other => panic!("unexpected output {other:?}"),
                })
                .collect()
        };
        let outputs = worker
            .predict(&inputs, Some(&grant), None)
            .await
            .expect("granted predict");
        assert_eq!(
            sizes(&outputs),
            vec![2, 2, 2, 2, 1],
            "one output per input, packed into batches of 2, 2, 1"
        );

        let measurements: Vec<BatchMeasurement> = telemetry
            .lock()
            .unwrap()
            .measurements()
            .map(|sample| sample.measurement.clone())
            .collect();
        assert_eq!(
            measurements
                .iter()
                .map(|batch| (batch.items, batch.units))
                .collect::<Vec<_>>(),
            vec![(Some(2), Some(2)), (Some(2), Some(2)), (Some(1), Some(1))],
            "one measurement per GPU batch; item/count prices one unit per item"
        );
        assert!(
            measurements.iter().all(|batch| !batch.oom
                && !batch.throughput_collapse
                && batch.duration_ms.is_some()),
            "clean batches, each individually timed: {measurements:?}"
        );

        // The user cap is an item-count constraint at pack time, overriding
        // the unit budget.
        let capped = Grant {
            unit_budget: 8,
            user_cap_items: Some(1),
            ..grant
        };
        let outputs = worker
            .predict(&inputs, Some(&capped), None)
            .await
            .expect("capped predict");
        assert_eq!(sizes(&outputs), vec![1, 1, 1, 1, 1]);

        worker.shutdown().await.expect("graceful shutdown");
    }

    /// The negative-sample path end to end: a granted window whose second GPU
    /// batch OOMs fails as a whole while the worker stays alive, and still
    /// reports both measurements on the error frame, the failing one flagged
    /// `oom` and its message carrying the prefix the ledger classifies on.
    #[tokio::test]
    async fn a_granted_window_reports_its_oom_batch_on_the_error_frame() {
        let mut worker = loaded("test/oomsecond", "oom_second_batch_test").await;
        let telemetry = worker.telemetry();

        let inputs: Vec<WorkerInput> = (0..4).flat_map(|index| one(json!(index))).collect();
        let err = worker
            .predict(&inputs, Some(&item_grant(2)), None)
            .await
            .expect_err("the second batch OOMs, so the window fails");
        let message = &err
            .downcast_ref::<WorkerError>()
            .expect("a per-request failure: the worker survives it")
            .message;
        assert!(message.contains("INFERENCE_OOM_WINDOW:"), "{message}");
        assert!(
            super::super::ledger::message_reports_oom(message),
            "the ledger classifies it as a negative sample: {message}"
        );

        let measurements: Vec<BatchMeasurement> = telemetry
            .lock()
            .unwrap()
            .measurements()
            .map(|sample| sample.measurement.clone())
            .collect();
        // A failed batch is never priced: its peaks stop where the call gave
        // up, so pricing it would feed the fit an under-stated cost.
        assert_eq!(
            measurements
                .iter()
                .map(|batch| (batch.oom, batch.units))
                .collect::<Vec<_>>(),
            vec![(false, Some(2)), (true, None)],
            "telemetry is recorded from error frames too: {measurements:?}"
        );

        worker
            .ping()
            .await
            .expect("the worker is still serviceable");
        worker.shutdown().await.expect("graceful shutdown");
    }

    /// A handshake naming an unknown impl_class fails the spawn with a
    /// WorkerError carrying the worker's own message and traceback; the child
    /// is killed and reaped (the test not hanging is the observable half).
    #[tokio::test]
    async fn spawn_unknown_impl_class_surfaces_worker_traceback() {
        let cfg = test_spawn_config();
        let Err(err) =
            Worker::spawn_configured(&cfg, "test/missing", &spec("does_not_exist"), None).await
        else {
            panic!("a handshake with an unknown impl_class must fail");
        };
        // The child is killed and reaped by the spawn error path; the test
        // finishing without a hang is the observable half of that.
        assert!(format!("{err:#}").contains("does_not_exist"), "{err:#}");
        let traceback = &err
            .downcast_ref::<WorkerError>()
            .expect("a handshake error frame maps to WorkerError")
            .traceback;
        assert!(traceback.contains("LookupError"), "{traceback}");
    }

    /// The thread that asks for a worker does not decide how long the worker
    /// lives: PR_SET_PDEATHSIG fires on the **forking thread's** exit, so
    /// spawning from a `std::thread` that then exits would kill the worker at
    /// once. It survives because the fork happens on the permanent spawner
    /// thread (`process_tree::spawn_supervised_tokio`).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_worker_outlives_the_thread_that_forked_it() {
        let handle = tokio::runtime::Handle::current();
        let mut worker =
            std::thread::spawn(move || handle.block_on(configured("test/echo", "echo_test")))
                .join()
                .expect("the forking thread finished");
        // The requesting thread is gone; a thread-scoped PDEATHSIG is
        // delivered on its `exit`, so this wait is generous, not a race.
        tokio::time::sleep(Duration::from_millis(500)).await;
        worker
            .ping()
            .await
            .expect("the worker outlived the thread that asked for it");
        assert!(worker.last_death().is_none(), "and nothing killed it since");
        worker.kill().await;
    }

    /// A worker is spawned as the kernel's first out-of-memory victim, so a
    /// batch that outgrows RAM kills it and no other program.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_worker_is_the_first_oom_victim() {
        let worker = loaded("test/echo", "echo_test").await;
        let pid = worker.pid.expect("a live worker has a pid");
        let adj = std::fs::read_to_string(format!("/proc/{pid}/oom_score_adj")).expect("readable");
        assert_eq!(adj.trim(), "1000");
        worker.shutdown().await.expect("graceful shutdown");
    }

    /// A worker killed externally while idle fails the next predict before a
    /// frame is sent, poisons the worker, and records *why* it is gone:
    /// signal, pid, status, stderr tail and attribution, gathered eagerly
    /// because the reap destroys them.
    #[tokio::test]
    async fn a_fatal_path_records_the_exit_signal_and_pid() {
        let mut worker = loaded("test/echo", "echo_test").await;
        assert!(
            worker.last_death().is_none(),
            "a live worker has no death record"
        );

        worker.kill_child_externally_for_test().await;
        let err = worker
            .predict(&one(json!(1)), None, None)
            .await
            .expect_err("predict against a dead worker must fail");
        assert!(
            err.downcast_ref::<WorkerError>().is_none(),
            "process death is a fatal supervision error, not an error frame"
        );
        assert!(
            format!("{err:#}").contains("process status"),
            "the error reports the exit status and stderr tail: {err:#}"
        );
        // Poisoned: further requests fail fast rather than hanging.
        let err = worker.ping().await.expect_err("dead worker stays dead");
        assert!(format!("{err:#}").contains("dead"));
        assert!(
            worker.take_death().is_none(),
            "it did not die running the request"
        );

        let death = worker
            .last_death()
            .expect("the fatal path recorded the death");
        assert_eq!(death.worker, "test/echo", "labelled with the model");
        assert!(death.pid.is_some(), "the pid was latched before the reap");
        assert!(death.status.is_some(), "reaped, status kept: {death}");
        assert!(
            death.why.contains("had exited before this predict request"),
            "the record says what the orchestrator was doing: {}",
            death.why
        );
        #[cfg(unix)]
        {
            // The hook SIGKILLs, the shape an out-of-memory kill takes.
            assert_eq!(death.signal, Some(9), "{death}");
            assert!(!death.core_dumped);
        }
        // Already gone and reapable when the fatal path reached it, so the
        // signal in the record is the one that actually killed it.
        assert_eq!(death.attribution, DeathAttribution::ReapedBeforeSignal);
        assert!(!death.attribution.killed_by_gateway());
        // And it renders the way the logs and the error chain show it.
        assert!(format!("{death}").contains("process status"));
    }

    /// The fatal path SIGKILLs the process group itself, so a desync on a
    /// **live** worker reaps as `signal: 9` too — the same shape a kernel OOM
    /// kill takes. The pre-signal attribution is what separates them.
    #[tokio::test]
    async fn a_gateway_kill_of_a_live_worker_is_not_reported_as_an_outside_kill() {
        let mut worker = loaded("test/echo", "echo_test").await;

        // Alive and answering; only the stream is unusable.
        worker.strand_in_flight_for_test();
        worker
            .predict(&one(json!(1)), None, None)
            .await
            .expect_err("a desynchronized stream cannot be resynchronized");

        let death = worker
            .last_death()
            .expect("the desync poisoned the worker, which is a recorded death");
        assert_eq!(death.attribution, DeathAttribution::StillRunning, "{death}");
        assert!(death.attribution.killed_by_gateway());
        #[cfg(unix)]
        assert_eq!(death.signal, Some(9), "our SIGKILL, in the status: {death}");
    }

    /// A worker the kernel killed whose thread-group leader is not reapable
    /// yet: `waitpid(WNOHANG)` refuses to report it, so it must be attributed
    /// `Dying` rather than to the gateway. The hook stands in for that kernel
    /// state; the process is really killed from outside.
    #[tokio::test]
    async fn a_kernel_kill_is_not_blamed_on_the_gateway_when_the_leader_reaps_late() {
        let mut worker = loaded("test/echo", "echo_test").await;

        worker.kill_child_externally_for_test().await;
        // From here `try_wait` answers nothing, exactly as it does for a
        // leader whose CUDA threads are still unwinding.
        worker.hide_exit_for_test();
        worker
            .predict(&one(json!(1)), None, None)
            .await
            .expect_err("predict against a dead worker must fail");

        // The stream was at EOF before the gateway signalled anything, so the
        // worker was already going down — an outside kill, not one of ours.
        let death = worker.last_death().expect("the fatal path recorded it");
        assert_eq!(death.attribution, DeathAttribution::Dying, "{death}");
        assert!(!death.attribution.killed_by_gateway());
        // `/proc` alone showed it gone before the request was sent, so it
        // did not die running it.
        #[cfg(target_os = "linux")]
        {
            assert!(death.why.contains("had exited before"), "{}", death.why);
            assert!(worker.take_death().is_none());
        }
    }

    /// A SIGKILL or a Windows out-of-memory status is a memory kill and any
    /// other exit a crash, unless the gateway killed the process.
    #[test]
    fn the_exit_status_tells_a_memory_kill_from_a_crash() {
        use DeathAttribution::{Dying, ReapedBeforeSignal, StillRunning};
        use DeathKind::{Crash, MemoryKill};
        let windows = |status: u32| Some(status as i32);
        for (attribution, signal, code, kind) in [
            (ReapedBeforeSignal, Some(9), None, Some(MemoryKill)),
            (Dying, Some(9), None, Some(MemoryKill)),
            (Dying, None, Some(3), Some(Crash)),
            (Dying, Some(11), None, Some(Crash)),
            (StillRunning, Some(9), None, None),
            (ReapedBeforeSignal, Some(11), None, Some(Crash)),
            (ReapedBeforeSignal, None, Some(3), Some(Crash)),
            (
                ReapedBeforeSignal,
                None,
                windows(0xC000_0017),
                Some(MemoryKill),
            ),
            (Dying, None, windows(0xC000_012D), Some(MemoryKill)),
            (ReapedBeforeSignal, None, windows(0xC000_0005), Some(Crash)),
            (StillRunning, None, windows(0xC000_0017), None),
        ] {
            assert_eq!(
                death_kind(attribution, signal, code),
                kind,
                "{attribution} {signal:?} {code:?}"
            );
        }
    }

    /// The `/proc` probe on this process and on a pid that cannot exist.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_proc_probe_separates_a_live_leader_from_a_vanished_one() {
        assert!(!leader_is_unwinding(Some(std::process::id())));
        assert!(!leader_is_unwinding(None), "no pid is not evidence");
        assert!(leader_is_unwinding(Some(u32::MAX)));
        // Only "not there" and a zombie state mean exited; a read that
        // failed for any other reason leaves a live worker alone.
        use std::io::{Error, ErrorKind};
        assert!(stat_reads_exited(Err(Error::from(ErrorKind::NotFound))));
        assert!(!stat_reads_exited(Err(Error::from_raw_os_error(
            libc::EMFILE
        ))));
        assert!(!stat_reads_exited(Err(Error::from(
            ErrorKind::PermissionDenied
        ))));
        assert!(stat_reads_exited(Ok("7 (python) Z 1 7".to_owned())));
        assert!(!stat_reads_exited(Ok("7 (python) S 1 7".to_owned())));
    }

    /// The attribution probes against an unread response frame in stdout: a
    /// live worker with one is not at EOF (right answer), but the bytes stay
    /// readable after a kernel kill, so the EOF probe alone would then blame
    /// the gateway and the `/proc` probe is what keeps it honest. Neither may
    /// consume the frame, so the last assertion reads it back.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_stranded_frame_does_not_make_a_kernel_kill_look_like_ours() {
        let mut worker = configured("test/echo", "echo_test").await;
        // A request whose answer nobody will read: the shape a cancelled
        // request future leaves behind.
        let bytes = encode_frame(&Value::Map(vec![
            (Value::from("type"), Value::from("ping")),
            (Value::from("id"), Value::from(9_999u64)),
        ]))
        .expect("a ping frame encodes");
        send_bytes(&mut worker.stdin, &bytes)
            .await
            .expect("the live worker takes the request");
        // Long enough for the answer to be in the pipe rather than in
        // flight; the assertions below do not race it either way.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let alive = worker.attribute_death().await;
        assert_eq!(
            alive,
            DeathAttribution::StillRunning,
            "unread answer, alive"
        );

        // Really killed from outside, with those bytes still readable, so the
        // EOF probe cannot see this death and the /proc probe is what stops
        // the gateway claiming the kill.
        worker.kill_child_externally_for_test().await;
        worker.hide_exit_for_test();
        assert!(!worker.stdout_at_eof().await, "the frame is still there");
        assert_eq!(worker.attribute_death().await, DeathAttribution::Dying);

        let frame = read_frame(&mut worker.stdout)
            .await
            .expect("neither probe consumed the frame");
        let Value::Map(map) = frame else {
            panic!("the worker answers with a map");
        };
        assert_eq!(map_get(&map, "id").and_then(Value::as_u64), Some(9_999));
    }

    /// stdout hygiene end to end: the fixture print()s during
    /// load/predict/unload and fd 1 is dup2'd to stderr before impl code runs,
    /// so every frame still parses and all three strings land in the tail.
    #[tokio::test]
    async fn stdout_hygiene_survives_printing_impl() {
        let mut worker = loaded("test/printer", "printing_test").await;

        let inputs = [one(json!(1))[0].clone(), one(json!(2))[0].clone()];
        let outputs = worker
            .predict(&inputs, None, None)
            .await
            .expect("predict ok");
        assert_eq!(
            outputs,
            vec![WorkerOutput::Json(json!({"printed": true})); 2]
        );

        // Keep a handle on the shared tail: shutdown() consumes the worker,
        // and the unload print only arrives during the graceful stop.
        let tail = Arc::clone(&worker.stderr);
        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
        let text = tail.lock().unwrap().snapshot();
        for step in ["load", "predict", "unload"] {
            let expected = format!("garbage on {step} stdout");
            assert!(text.contains(&expected), "{expected:?} missing from {text}");
        }
    }

    /// The stderr forwarder survives arbitrary bytes: the fixture writes raw
    /// invalid UTF-8 and a >64 KiB \r-only run to fd 2 during predict. A
    /// forwarder that died there would fill the pipe and hang the
    /// deadline-less predict forever; the marker written *after* the garbage
    /// is proof it kept reading.
    #[tokio::test]
    async fn stderr_forwarder_survives_invalid_utf8_and_cr_only_runs() {
        let mut worker = loaded("test/badbytes", "badbytes_test").await;

        // The second predict proves the worker and its stderr pipe are still
        // fully serviceable after the garbage.
        let input = one(json!(1));
        for attempt in ["first", "second"] {
            let outputs = worker
                .predict(&input, None, None)
                .await
                .unwrap_or_else(|err| panic!("{attempt} predict: {err:#}"));
            assert_eq!(outputs, vec![WorkerOutput::Json(json!({"bad": true}))]);
        }

        // The forwarder drains asynchronously; poll for the marker line the
        // fixture writes after the invalid bytes and the \r run.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !worker
            .stderr_tail_snapshot()
            .contains("marker-after-bad-bytes")
        {
            assert!(
                tokio::time::Instant::now() <= deadline,
                "the stderr tail never captured the post-garbage marker: {:?}",
                worker.stderr_tail_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// The msgpack half of the error-slot decoder: only a map carrying the
    /// reserved key is a slot error, a malformed body is a violation rather
    /// than a guess, and ordinary payloads are left alone.
    #[test]
    fn error_slot_from_rmpv_accepts_only_the_documented_shape() {
        let slot_of = |body: Value| Value::Map(vec![(Value::from(ERROR_SLOT_KEY), body)]);
        let map_of = |fields: Vec<(&str, &str)>| {
            Value::Map(
                fields
                    .into_iter()
                    .map(|(key, value)| (Value::from(key), Value::from(value)))
                    .collect(),
            )
        };
        let whole = slot_of(map_of(vec![
            ("class", "input"),
            ("message", "Unreadable image"),
        ]));
        assert_eq!(
            error_slot_from_rmpv(&whole),
            Some(Ok(SlotError {
                class: super::super::slot_error::SlotErrorClass::Input,
                message: "Unreadable image".to_owned(),
            }))
        );

        // Ordinary payloads, including maps with other keys, are left alone.
        for payload in [
            Value::Binary(vec![1, 2]),
            Value::from("text"),
            map_of(vec![("tags", "a")]),
        ] {
            assert_eq!(error_slot_from_rmpv(&payload), None, "{payload}");
        }

        // A body that is not a map, an unknown class, and a class with no
        // message: all violations, none of them guessed at.
        for malformed in [
            slot_of(Value::from("boom")),
            slot_of(map_of(vec![("class", "blocked")])),
            slot_of(map_of(vec![("class", "input")])),
        ] {
            assert!(
                matches!(error_slot_from_rmpv(&malformed), Some(Err(_))),
                "{malformed} must be rejected"
            );
        }
    }

    /// Per-item error slots end to end: a batch mixing two typed failures with
    /// healthy JSON and binary outputs comes back with every slot in its
    /// input's position — a shifted slot would blame the wrong file — and the
    /// worker keeps serving, since an error slot is a successful roundtrip.
    #[tokio::test]
    async fn error_slots_decode_and_stay_aligned_with_healthy_outputs() {
        let mut worker = loaded("test/errorslot", "errorslot_test").await;

        let inputs = [
            one(json!("first"))[0].clone(),
            one(json!("bad"))[0].clone(),
            WorkerInput {
                data: None,
                file: Some(b"payload".to_vec()),
            },
            one(json!("flaky"))[0].clone(),
        ];
        let outputs = worker
            .predict(&inputs, None, None)
            .await
            .expect("predict ok");
        let slot = |class, message: &str| {
            WorkerOutput::Error(SlotError {
                class,
                message: message.to_owned(),
            })
        };
        use super::super::slot_error::SlotErrorClass::{Input, Transient};
        assert_eq!(
            outputs,
            vec![
                WorkerOutput::Json(json!({"ok": "first"})),
                slot(Input, "Unreadable image: truncated"),
                WorkerOutput::Bytes(b"bytes:payload".to_vec()),
                slot(Transient, "try again"),
            ],
            "one slot per input, each in its input's position"
        );

        // Nothing about the worker changed: it keeps serving.
        let outputs = worker
            .predict(&one(json!("again")), None, None)
            .await
            .expect("worker is still serviceable");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": "again"}))]);
        worker.shutdown().await.expect("graceful shutdown");
    }

    /// A reserved key with an undefined body is a protocol violation: the
    /// worker is killed and poisoned rather than the class being guessed.
    #[tokio::test]
    async fn a_malformed_error_slot_kills_the_worker() {
        let mut worker = loaded("test/errorslot", "errorslot_test").await;

        let err = worker
            .predict(&one(json!("malformed")), None, None)
            .await
            .expect_err("a malformed error slot must fail the predict");
        assert!(
            format!("{err:#}").contains("malformed error slot"),
            "{err:#}"
        );
        assert!(
            err.downcast_ref::<WorkerError>().is_none(),
            "a protocol violation is fatal, not a per-request worker error"
        );
        let err = worker.ping().await.expect_err("the worker is poisoned");
        assert!(format!("{err:#}").contains("dead"));
    }

    /// Non-finite floats and nested binary/ext have no JSON form:
    /// `rmpv_to_json` errors rather than silently coercing.
    #[test]
    fn rmpv_to_json_rejects_nonfinite_and_nested_binary() {
        assert!(rmpv_to_json(&Value::F64(f64::NAN)).is_err());
        assert!(rmpv_to_json(&Value::F64(f64::INFINITY)).is_err());
        assert!(rmpv_to_json(&Value::F32(f32::NEG_INFINITY)).is_err());
        assert!(rmpv_to_json(&Value::Array(vec![Value::Binary(vec![1, 2])])).is_err());
        assert!(rmpv_to_json(&Value::Ext(7, vec![0])).is_err());
        assert_eq!(rmpv_to_json(&Value::F64(1.5)).unwrap(), json!(1.5));
    }

    /// The pooled flow end to end: spawn by impl class, prewarm, park (ping,
    /// as the orchestrator does before claiming), then configure + load +
    /// predict. The fixture reports the flag `prepare()` set, so
    /// `{"prepared": true}` proves the prewarm ran before the model was bound.
    #[tokio::test]
    async fn prewarm_park_configure_load_happy_path() {
        let mut worker = Worker::spawn(&test_spawn_config(), "prepare_test", None)
            .await
            .expect("spawn + identity handshake");
        worker.prewarm().await.expect("prewarm runs prepare()");

        // Parked: the worker is idle and unbound; ping is the claim check.
        worker.ping().await.expect("parked worker answers ping");

        worker
            .configure("test/prepare", &json!({}))
            .await
            .expect("configure instantiates after the park");
        worker.load().await.expect("load ok");
        let outputs = worker
            .predict(&one(json!(1)), None, None)
            .await
            .expect("predict ok");
        assert_eq!(
            outputs,
            vec![WorkerOutput::Json(
                json!({"prepared": true, "device": null})
            )]
        );

        // The prepare() stderr marker was forwarded; the tail drains
        // asynchronously, so poll briefly for it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !worker
            .stderr_tail_snapshot()
            .contains("prepare_test-prepare-marker")
        {
            assert!(
                tokio::time::Instant::now() <= deadline,
                "prepare() marker never reached the stderr tail: {:?}",
                worker.stderr_tail_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0), "worker exits 0 after unload");
    }

    /// unload is valid in every state: a prewarmed but never-configured
    /// worker is dismissed through the same graceful ladder and exits 0.
    #[tokio::test]
    async fn parked_worker_unloads_gracefully() {
        let mut worker = Worker::spawn(&test_spawn_config(), "echo_test", None)
            .await
            .expect("spawn + identity handshake");
        worker.prewarm().await.expect("prewarm (no prepare) is ok");
        let status = worker.shutdown().await.expect("shutdown while parked");
        assert_eq!(status.code(), Some(0), "parked worker exits 0 on unload");
    }

    /// State-machine errors are per-request and never poison the worker: a
    /// predict before configure, a predict before load, and a double
    /// configure are all WorkerErrors the same process serves through.
    #[tokio::test]
    async fn failed_configure_does_not_poison_worker() {
        let mut worker = Worker::spawn(&test_spawn_config(), "prepare_test", None)
            .await
            .expect("spawn + identity handshake");

        // Each of these is an `error` frame the same process serves through;
        // the message says which step of the state machine was missed.
        async fn refused(worker: &mut Worker, expected: &str) {
            let err = worker
                .predict(&one(json!(1)), None, None)
                .await
                .expect_err("a state-machine violation must fail");
            let message = &err
                .downcast_ref::<WorkerError>()
                .expect("per-request failure maps to WorkerError")
                .message;
            assert!(message.contains(expected), "{message}");
        }
        refused(&mut worker, "configure").await;
        worker
            .configure("test/prepare", &json!({}))
            .await
            .expect("configure still works on the same worker");
        worker
            .configure("test/prepare-again", &json!({}))
            .await
            .expect_err("second configure is a per-request error")
            .downcast_ref::<WorkerError>()
            .expect("double configure maps to WorkerError");
        refused(&mut worker, "load").await;
        worker.load().await.expect("first instance is intact");
        worker.ping().await.expect("worker is still serviceable");

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// A harness answering the handshake with protocol_version 1 fails the
    /// spawn with a fatal error naming the version; the fake lingers on stdin,
    /// so the test finishing without a hang is the observable half of the kill.
    #[tokio::test]
    async fn version_mismatch_kills_worker() {
        let mut cfg = test_spawn_config();
        cfg.pythonpath.insert(
            0,
            workspace_root().join("python/tests/inferio_worker/fake_v1_harness"),
        );
        let Err(err) = Worker::spawn(&cfg, "echo_test", None).await else {
            panic!("a v1 handshake echo must be rejected");
        };
        let text = format!("{err:#}");
        assert!(
            text.contains("protocol_version") && text.contains("expected 2"),
            "the error names the version mismatch: {text}"
        );
        assert!(
            err.downcast_ref::<WorkerError>().is_none(),
            "a version mismatch is fatal supervision, not an error frame"
        );
    }

    /// A worker on the fake harness that writes the mid-request frame sequence
    /// named by `mode` before each `predict` reply, already loaded.
    async fn frame_harness(mode: &str, env: Vec<(&str, &str)>) -> Result<Worker> {
        let mut cfg = test_spawn_config();
        cfg.pythonpath.insert(
            0,
            workspace_root().join("python/tests/inferio_worker/fake_memory_frames_harness"),
        );
        cfg.env
            .push(("INFERIO_FAKE_FRAMES".to_owned(), mode.to_owned()));
        cfg.env.extend(
            env.into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned())),
        );
        let mut worker =
            Worker::spawn_configured(&cfg, "test/frames", &spec("echo_test"), None).await?;
        worker.load().await?;
        Ok(worker)
    }

    /// The pool figure and free reading the worker's telemetry currently holds.
    fn pool_and_free(worker: &Worker) -> (Option<u64>, Option<u64>) {
        let telemetry = worker.telemetry();
        let telemetry = telemetry.lock().expect("telemetry lock");
        let sample = telemetry
            .memory
            .as_ref()
            .map(|stamped| stamped.value.clone());
        (
            sample.as_ref().and_then(|sample| sample.reserved_mb),
            sample.and_then(|sample| sample.free_mb),
        )
    }

    /// The frames a granted window writes *before* its reply are folded into
    /// telemetry and are not the reply: the outputs still arrive intact and in
    /// order, and the ledger sees this replica's pool as of its last batch
    /// rather than as of its last response. This is the whole point of the
    /// frame — a replica 60 s into a window otherwise reports nothing while a
    /// neighbour's replies keep the device-wide free reading fresh.
    #[tokio::test]
    async fn per_batch_memory_frames_refresh_the_pool_before_the_reply_lands() {
        // Reply-level sample suppressed, so what is read back is the frames'.
        let mut worker = frame_harness(
            "memory",
            vec![
                ("INFERIO_FAKE_FRAME_COUNT", "3"),
                ("INFERIO_FAKE_REPLY_MEMORY", "0"),
            ],
        )
        .await
        .expect("the fake harness comes up");
        let outputs = worker
            .predict(&one(json!("x")), Some(&item_grant(4)), None)
            .await
            .expect("three memory frames then ok is one clean roundtrip");
        assert_eq!(outputs.len(), 1, "the frames are not responses");
        assert_eq!(
            pool_and_free(&worker),
            (Some(300), Some(4096 - 300)),
            "the last frame's sample, pool and free together"
        );

        // And the reply's own sample still wins when it carries one: it is the
        // freshest reading in the exchange, taken after the final batch.
        let mut worker = frame_harness("memory", vec![("INFERIO_FAKE_FRAME_COUNT", "3")])
            .await
            .expect("the fake harness comes up");
        worker
            .predict(&one(json!("x")), Some(&item_grant(4)), None)
            .await
            .expect("ok");
        assert_eq!(pool_and_free(&worker).0, Some(400));
        worker.kill().await;
    }

    /// Both directions of the skew, and the two frames that must stay fatal.
    ///
    /// A worker predating the capability sends nothing and is served exactly
    /// as before. A `memory` frame for another id is a desynchronized stream,
    /// and so is any other unexpected type for the id in flight — neither
    /// reading changed when the reader learned to loop.
    #[tokio::test]
    async fn only_a_memory_frame_for_the_id_in_flight_is_tolerated() {
        let mut silent = frame_harness("silent", Vec::new())
            .await
            .expect("a worker that never sends frames comes up");
        let outputs = silent
            .predict(&one(json!("x")), Some(&item_grant(4)), None)
            .await
            .expect("an old worker's stream is untouched");
        assert_eq!(outputs.len(), 1);
        assert_eq!(pool_and_free(&silent).0, Some(300), "the reply's sample");
        silent.kill().await;

        for (mode, label, verdict) in [
            (
                "foreign",
                "a memory frame for another id",
                "does not match request id",
            ),
            (
                "unknown",
                "an unexpected frame type for the id in flight",
                "unexpected response frame type",
            ),
        ] {
            let mut worker = frame_harness(mode, Vec::new())
                .await
                .expect("the fake harness comes up");
            let err = worker
                .predict(&one(json!("x")), Some(&item_grant(4)), None)
                .await
                .expect_err(label);
            let text = format!("{err:#}");
            assert!(text.contains(verdict), "{label}: {text}");
            assert!(
                err.downcast_ref::<WorkerError>().is_none(),
                "{label} is fatal supervision, not an error frame"
            );
            let after = format!("{:#}", worker.ping().await.expect_err(label));
            assert!(
                after.contains("dead after a previous fatal error"),
                "{label} poisons the worker, as a desynchronized stream must: {after}"
            );
            worker.kill().await;
        }
    }

    /// The host announces the capability in its handshake. Asserted against a
    /// harness that refuses to come up without it, so the assertion is on the
    /// bytes the orchestrator actually wrote.
    #[tokio::test]
    async fn the_handshake_announces_the_batch_memory_frame_capability() {
        let mut worker = frame_harness("require", Vec::new())
            .await
            .expect("the handshake carried batch_memory_frames: true");
        worker
            .predict(&one(json!("x")), Some(&item_grant(4)), None)
            .await
            .expect("ok");
        worker.kill().await;
    }

    fn measurement(items: u64) -> BatchMeasurement {
        BatchMeasurement {
            items: Some(items),
            ..BatchMeasurement::default()
        }
    }

    /// Every measurement is kept, not just the last response's: a bounded ring
    /// with per-worker sequence numbers, not a last-write-wins slot. The
    /// numbers keep climbing across evictions, which is what lets a watermark
    /// reader notice it lost samples instead of assuming continuity.
    #[test]
    fn successive_predicts_retain_every_measurement() {
        let mut telemetry = WorkerTelemetry::default();
        for items in 1..=3 {
            telemetry.record_measurements(vec![measurement(items)]);
        }
        assert_eq!(telemetry.recorded_measurements(), 3);
        assert_eq!(
            telemetry
                .measurements()
                .map(|sample| (sample.seq, sample.measurement.items))
                .collect::<Vec<_>>(),
            vec![(1, Some(1)), (2, Some(2)), (3, Some(3))],
            "oldest first, one entry per measurement"
        );

        // One response carrying several GPU batches contributes one sample
        // each, not one for the response.
        telemetry.record_measurements(vec![measurement(10), measurement(11)]);
        assert_eq!(telemetry.recorded_measurements(), 5);
        assert_eq!(telemetry.measurements().next_back().map(|s| s.seq), Some(5));

        // Overflow: the ring is bounded, the counter is not, and the oldest
        // retained seq moves past 1 — which is what makes a stale watermark
        // detectable as a gap rather than read as continuity.
        for items in 0..WorkerTelemetry::RING as u64 {
            telemetry.record_measurements(vec![measurement(items)]);
        }
        let total = 5 + WorkerTelemetry::RING as u64;
        assert_eq!(telemetry.measurements().count(), WorkerTelemetry::RING);
        assert_eq!(telemetry.recorded_measurements(), total);
        assert!(telemetry.measurements().next().expect("full").seq > 1);
        assert_eq!(
            telemetry.measurements().next_back().map(|s| s.seq),
            Some(total),
            "the newest sample's seq is the running count"
        );
    }

    /// The grant states the model's two per-item caps on the wire, so the
    /// worker prices `min(raw, cap)` against the number the host sized the
    /// window with. Nil — never omitted — when there is none.
    #[test]
    fn the_grant_states_the_models_per_item_caps() {
        let grant = |canvas_pixels, max_tokens| Grant {
            unit_budget: 8,
            mb: 512,
            unit: super::super::cost::CostUnit::Pixel,
            aggregation: super::super::cost::CostAggregation::Sum,
            user_cap_items: None,
            canvas_pixels,
            max_tokens,
            squeezed: false,
            fixed_mb: 0,
            ram_mb: 0,
            ram_reserve_mb: 0,
        };
        let on_the_wire = |key: &str, canvas_pixels, max_tokens| {
            let encoded = encode_grant(&grant(canvas_pixels, max_tokens));
            let Value::Map(map) = &encoded else {
                panic!("a grant encodes as a map, got {encoded:?}");
            };
            map_get(map, key).cloned()
        };
        assert_eq!(
            on_the_wire("canvas_pixels", Some(1_835_008), None),
            Some(Value::from(1_835_008u64))
        );
        assert_eq!(
            on_the_wire("max_tokens", None, Some(256)),
            Some(Value::from(256u64))
        );
        assert_eq!(
            on_the_wire("canvas_pixels", None, None),
            Some(Value::Nil),
            "present and nil, not absent"
        );
        assert_eq!(
            on_the_wire("max_tokens", None, None),
            Some(Value::Nil),
            "present and nil, not absent"
        );

        // The fixed part of the price, the host RAM a window may take and
        // the reserve its clamp keeps.
        let encoded = encode_grant(&Grant {
            fixed_mb: 35,
            ram_mb: 4_100,
            ram_reserve_mb: 6_553,
            ..grant(None, None)
        });
        let Value::Map(map) = &encoded else {
            panic!("a grant encodes as a map, got {encoded:?}");
        };
        assert_eq!(map_get(map, "fixed_mb"), Some(&Value::from(35u64)));
        assert_eq!(map_get(map, "ram_mb"), Some(&Value::from(4_100u64)));
        assert_eq!(map_get(map, "ram_reserve_mb"), Some(&Value::from(6_553u64)));
    }

    /// The other direction: the canvas the worker resolved for the model it
    /// just loaded, parsed with the same suspicion as every other field — a
    /// nonsense value reads as unknown, never as a cap.
    #[test]
    fn load_report_carries_the_resolved_canvas() {
        let with_base = |value: Value| {
            LoadReport::parse(&[
                (Value::from("base_mb"), Value::from(2048u64)),
                (Value::from("canvas_pixels"), value),
            ])
            .expect("the base still parses")
            .canvas_pixels
        };
        assert_eq!(with_base(Value::from(11_289_600u64)), Some(11_289_600));
        for bad in [
            Value::from("1843200"),
            Value::from(0u64),
            Value::from(-1i64),
            Value::from(u64::from(u32::MAX) + 1),
            Value::Nil,
        ] {
            assert_eq!(with_base(bad.clone()), None, "{bad:?}");
        }

        // The canvas alone is a report: an impl that measured no footprint at
        // all still has a geometry the host can price with.
        let alone = vec![(Value::from("canvas_pixels"), Value::from(1_843_200u64))];
        assert_eq!(
            LoadReport::parse(&alone).and_then(|report| report.canvas_pixels),
            Some(1_843_200)
        );

        // The token window travels the same direction under the same rules.
        let window = |value: Value| {
            LoadReport::parse(&[
                (Value::from("base_mb"), Value::from(2048u64)),
                (Value::from("max_tokens"), value),
            ])
            .expect("the base still parses")
            .max_tokens
        };
        assert_eq!(window(Value::from(256u64)), Some(256));
        for bad in [
            Value::from("256"),
            Value::from(0u64),
            Value::from(-1i64),
            Value::from(u64::from(u32::MAX) + 1),
            Value::Nil,
        ] {
            assert_eq!(window(bad.clone()), None, "{bad:?}");
        }
    }

    /// The device the worker says it ran on: the field the ledger places the
    /// replica by, so a CPU interpreter on a CUDA host lands on the CPU
    /// device. A worker too old to send it, or one with no torch, says
    /// nothing and the identity fields decide as before.
    #[test]
    fn load_report_carries_the_device_kind() {
        let kind = |value: Value| {
            LoadReport::parse(&[
                (Value::from("base_mb"), Value::from(2048u64)),
                (Value::from("device_kind"), value),
            ])
            .expect("the base still parses")
            .device_kind
        };
        assert_eq!(kind(Value::from("cpu")).as_deref(), Some("cpu"));
        assert_eq!(kind(Value::from("cuda")).as_deref(), Some("cuda"));
        assert_eq!(kind(Value::from(7i64)), None);
        assert_eq!(kind(Value::Nil), None);
        assert_eq!(
            LoadReport::parse(&[(Value::from("base_mb"), Value::from(2048u64))])
                .expect("an older worker still reports")
                .device_kind,
            None
        );
    }

    /// The worker's response map is untrusted input: a wrong type, a negative
    /// count or a nil where a map belongs reads as "unknown", never as a wrong
    /// number and never as a protocol failure. Includes the ROCm identity
    /// pair, which a worker with no `gpu_uuid` is keyed and cross-checked by.
    #[test]
    fn load_report_parse_tolerates_wrong_types() {
        let parse = |fields: Vec<(&str, Value)>| {
            let map: Vec<_> = fields
                .into_iter()
                .map(|(key, value)| (Value::from(key), value))
                .collect();
            LoadReport::parse(&map)
        };
        #[rustfmt::skip]
        let garbage = vec![
            ("base_mb", Value::from("4321")), ("base_method", Value::from(7i64)),
            ("reserved_at_load_mb", Value::from(-5i64)), ("dtype", Value::from(2.5f64)),
            ("gpu_uuid", Value::Nil), ("memory", Value::Nil),
        ];
        assert_eq!(
            parse(garbage),
            None,
            "nothing usable is the same as an older worker reporting nothing"
        );

        // `memory` as an array (not a map) is ignored while the good fields
        // around it survive; a stringified total is unknown, never a parsed
        // number, since the registration cross-check must not admit a GPU on
        // a guess.
        #[rustfmt::skip]
        let mixed = vec![
            ("base_mb", Value::from(4321u64)), ("base_method", Value::from("nvml")),
            ("memory", Value::Array(vec![Value::from(1u64)])),
            ("allocated_at_load_mb", Value::from(900u64)),
            ("gpu_uuid", Value::from("GPU-1a2b")), ("gpu_name", Value::from(42i64)),
            ("gpu_arch", Value::from(120i64)),
            ("gpu_bdf", Value::from(3i64)), ("gpu_total_mb", Value::from("24576")),
            ("torch_version", Value::from("2.7.1+cu128")),
            ("rss_at_load_mb", Value::from(2900u64)),
        ];
        let report = parse(mixed).expect("the good fields are kept");
        assert_eq!(report.base_mb, Some(4321));
        assert_eq!(report.rss_at_load_mb, Some(2900));
        assert_eq!(report.base_method.as_deref(), Some("nvml"));
        assert_eq!(report.allocated_at_load_mb, Some(900));
        assert_eq!(
            report.reserved_at_load_mb, None,
            "the two baselines are independent: an absent one is unknown"
        );
        assert_eq!(report.gpu_uuid.as_deref(), Some("GPU-1a2b"));
        assert_eq!(report.torch_version.as_deref(), Some("2.7.1+cu128"));
        assert_eq!(
            (
                report.memory,
                report.gpu_name,
                report.gpu_arch,
                report.gpu_bdf,
                report.gpu_total_mb
            ),
            (None, None, None, None, None),
            "a malformed architecture is unknown, never a key made from a number"
        );

        // A whole-MiB float (a worker that ever switches to fractional MB)
        // rounds rather than reading as absent; a negative one is unknown.
        #[rustfmt::skip]
        let floats = vec![
            ("base_mb", Value::from(1536.4f64)),
            ("reserved_at_load_mb", Value::from(-1.0f64)),
            ("allocated_at_load_mb", Value::from(-1i64)),
        ];
        let report = parse(floats).expect("float base is usable");
        assert_eq!(report.base_mb, Some(1536));
        assert_eq!(report.reserved_at_load_mb, None);
        assert_eq!(report.allocated_at_load_mb, None);

        // A ROCm worker reports no `gpu_uuid` at all (torch renders a
        // third-vocabulary one on HIP and the worker suppresses it) and a PCI
        // address instead — the pair the ledger keys and cross-checks it by.
        #[rustfmt::skip]
        let rocm = vec![
            ("base_mb", Value::from(2048u64)), ("base_method", Value::from("alloc_delta")),
            ("gpu_bdf", Value::from("0000:03:00.0")), ("gpu_total_mb", Value::from(24_560u64)),
            ("gpu_name", Value::from("AMD Radeon RX 7900 XTX")),
            ("gpu_arch", Value::from("gfx1100")),
            ("torch_version", Value::from("2.11.0+rocm7.2")),
        ];
        let report = parse(rocm).expect("a report with no uuid is a report");
        assert_eq!(report.gpu_uuid, None);
        assert_eq!(report.gpu_bdf.as_deref(), Some("0000:03:00.0"));
        assert_eq!(report.gpu_total_mb, Some(24_560));
        assert_eq!(
            report.gpu_arch.as_deref(),
            Some("gfx1100"),
            "the profile key travels beside the SKU name, not instead of it"
        );
        // That pair alone is enough to make a report: a worker that could
        // measure nothing else still has an identity to register with.
        let identity = parse(vec![("gpu_bdf", Value::from("0000:0c:00.0"))]);
        assert_eq!(
            identity.map(|report| report.gpu_bdf),
            Some(Some("0000:0c:00.0".to_owned()))
        );
    }

    /// The measurement array is from the same untrusted source: non-map
    /// entries are skipped, and a non-array field yields no measurements.
    #[test]
    fn measurement_list_skips_non_map_entries() {
        #[rustfmt::skip]
        let list = Value::Array(vec![
            Value::Nil,
            Value::from(7i64),
            Value::Map(vec![
                (Value::from("items"), Value::from(8u64)),
                (Value::from("peak_reserved_mb"), Value::from(1200u64)),
                (Value::from("duration_ms"), Value::from(12.5f64)),
                (Value::from("spilled"), Value::from(true)),
                (Value::from("next_over_budget"), Value::from(true)),
            ]),
            Value::Array(vec![Value::from("items")]),
            Value::Map(vec![(Value::from("items"), Value::from("eight"))]),
        ]);
        let measurements = BatchMeasurement::parse_list(Some(&list));
        assert_eq!(measurements.len(), 2, "only the two maps became entries");
        let first = &measurements[0];
        assert_eq!(
            (
                first.items,
                first.peak_reserved_mb,
                first.duration_ms,
                first.spilled,
                first.next_over_budget
            ),
            (Some(8), Some(1200), Some(12.5), true, true)
        );
        assert_eq!(
            measurements[1],
            BatchMeasurement::default(),
            "a map with only unusable values is an all-unknown measurement"
        );
        // A non-array field is no measurements, not a panic.
        for absent in [None, Some(&Value::Nil), Some(&Value::from("x"))] {
            assert!(BatchMeasurement::parse_list(absent).is_empty());
        }
    }

    /// The post-batch pool and the RAM pair on the wire, and which of them a
    /// CUDA worker sends. `reserved_after_mb` is on **every** backend's frame;
    /// only the RAM pair is Metal-scoped. A frame from a worker too old for
    /// either — the shape below — parses with both unset and every other field
    /// intact, so the fields are additive to a reader.
    #[test]
    fn a_frame_without_the_post_batch_pool_or_ram_fields_still_parses() {
        #[rustfmt::skip]
        let old = Value::Array(vec![Value::Map(vec![
            (Value::from("items"), Value::from(8u64)),
            (Value::from("units"), Value::from(8u64)),
            (Value::from("reserved_before_mb"), Value::from(1000u64)),
            (Value::from("peak_reserved_mb"), Value::from(1400u64)),
            (Value::from("allocated_before_mb"), Value::from(900u64)),
            (Value::from("peak_allocated_mb"), Value::from(1300u64)),
            (Value::from("duration_ms"), Value::from(10.0f64)),
            (Value::from("free_mb"), Value::from(18000u64)),
            (Value::from("free_source"), Value::from("nvml")),
        ])]);
        let frame = &BatchMeasurement::parse_list(Some(&old))[0];
        assert_eq!(frame.reserved_after_mb, None);
        assert_eq!(frame.ram_total_mb, None);
        assert_eq!(frame.ram_available_mb, None);
        assert_eq!((frame.peak_rss_mb, frame.rss_after_mb), (None, None));
        assert_eq!(frame.peak_reserved_mb, Some(1400));
        assert_eq!(frame.free_source.as_deref(), Some("nvml"));

        // A current CUDA worker's frame: the post-batch pool, its resident
        // set, and no RAM pair.
        #[rustfmt::skip]
        let cuda = Value::Array(vec![Value::Map(vec![
            (Value::from("items"), Value::from(8u64)),
            (Value::from("reserved_before_mb"), Value::from(1000u64)),
            (Value::from("reserved_after_mb"), Value::from(1000u64)),
            (Value::from("peak_reserved_mb"), Value::from(1400u64)),
            (Value::from("free_mb"), Value::from(18000u64)),
            (Value::from("free_source"), Value::from("nvml")),
            (Value::from("peak_rss_mb"), Value::from(3400u64)),
            (Value::from("rss_after_mb"), Value::from(3100u64)),
        ])]);
        let frame = &BatchMeasurement::parse_list(Some(&cuda))[0];
        assert_eq!(frame.reserved_after_mb, Some(1000), "sent off MPS too");
        assert_eq!(frame.ram_total_mb, None);
        assert_eq!(frame.ram_available_mb, None);
        assert_eq!(
            (frame.peak_rss_mb, frame.rss_after_mb),
            (Some(3400), Some(3100))
        );

        // And the Metal frame, the only one that carries all three.
        #[rustfmt::skip]
        let mps = Value::Array(vec![Value::Map(vec![
            (Value::from("items"), Value::from(8u64)),
            (Value::from("reserved_before_mb"), Value::from(1000u64)),
            (Value::from("reserved_after_mb"), Value::from(1050u64)),
            (Value::from("peak_reserved_mb"), Value::from(1400u64)),
            (Value::from("free_mb"), Value::from(18000u64)),
            (Value::from("free_source"), Value::from("mps")),
            (Value::from("ram_total_mb"), Value::from(131072u64)),
            (Value::from("ram_available_mb"), Value::from(15891u64)),
        ])]);
        let frame = &BatchMeasurement::parse_list(Some(&mps))[0];
        assert_eq!(frame.reserved_after_mb, Some(1050));
        assert_eq!(frame.ram_total_mb, Some(131072));
        assert_eq!(frame.ram_available_mb, Some(15891));
    }

    /// The two clamps, including a shape ceiling that arrives without a free
    /// reading and must still be read.
    #[test]
    fn a_clamp_is_read_from_its_unit_counts_and_keeps_the_reason_it_names() {
        // (from_units, to_units, free_mb, reason) as the worker sends them.
        let clamp = |fields: &[(&str, Value)]| {
            let map: Vec<_> = fields
                .iter()
                .map(|(key, value)| (Value::from(*key), value.clone()))
                .collect();
            ClampReport::parse(Some(&Value::Map(map)))
        };
        let num = |key, units: u64| (key, Value::from(units));
        let text = |value: &'static str| ("reason", Value::from(value));

        // An absent reason means the memory clamp and stays absent. A shape
        // ceiling with no live reading is still a clamp. When both bind one
        // batch, `reason` names what set `to_units`. An empty reason is none.
        /// A labelled clamp map and the (from_units, to_units, free_mb,
        /// reason) it must read back as.
        type Case<'a> = (&'a str, Vec<(&'a str, Value)>, Read<'a>);
        type Read<'a> = (u64, u64, Option<u64>, Option<&'a str>);
        #[rustfmt::skip]
        let cases: [Case; 4] = [
            ("memory clamp", vec![num("from_units", 64), num("to_units", 16), num("free_mb", 900)], (64, 16, Some(900), None)),
            ("shape ceiling, no reading", vec![num("from_units", 6), num("to_units", 2), text("index_limit")], (6, 2, None, Some("index_limit"))),
            ("both at once", vec![num("from_units", 64), num("to_units", 2), num("free_mb", 8_000), text("index_limit")], (64, 2, Some(8_000), Some("index_limit"))),
            ("an empty reason", vec![num("from_units", 6), num("to_units", 2), text("")], (6, 2, None, None)),
        ];
        for (label, fields, expected) in cases {
            let got = clamp(&fields).unwrap_or_else(|| panic!("{label} is a clamp"));
            let got = (
                got.from_units,
                got.to_units,
                got.free_mb,
                got.reason.as_deref(),
            );
            assert_eq!(got, expected, "{label}");
        }

        // The unit counts are the statement, so a fragment missing either of
        // them is still not one: the ledger reads this map's presence as
        // "exclude this batch", which is too consequential to infer.
        assert!(clamp(&[num("to_units", 2)]).is_none());
        assert!(clamp(&[num("from_units", 6)]).is_none());
        assert!(ClampReport::parse(None).is_none());
        assert!(ClampReport::parse(Some(&Value::from("clamped"))).is_none());
    }
}
