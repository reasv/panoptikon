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
//! Failure semantics (design doc §4):
//! - `error` frames are per-request failures; the worker stays alive and the
//!   method returns a [`WorkerError`] (downcastable from the `anyhow` chain).
//! - Framing violations (oversized frame, garbage, id mismatch, unexpected
//!   type), deadline timeouts, and worker exit/EOF are fatal: the child is
//!   killed and reaped, the `Worker` is poisoned, and the error carries the
//!   exit status plus the stderr tail.
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

use super::ledger::{FitSnapshot, Grant};
use super::registry::SpawnSpec;
use super::slot_error::{ERROR_SLOT_KEY, SlotError, slot_error_from_parts};
use crate::process_tree::{JobGuard, detach_from_console, die_with_parent, kill_process_group};

/// Protocol version this orchestrator speaks; workers answering anything
/// else in the handshake are killed.
const PROTOCOL_VERSION: u64 = 2;

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

/// Deadline for a `trim` (protocol doc, "Lifecycle and timeouts").
///
/// A trim is best-effort hygiene and must never hold a dispatcher for minutes,
/// but the operation it performs is `cudaFree` over every block in the
/// allocator pool — which on a multi-gigabyte pool, on a busy board, under
/// WDDM, is not the milliseconds an idle `empty_cache()` costs. Timing out is
/// **fatal** (the worker is unresponsive and the stream would desynchronize),
/// so this budget has to be long enough that a slow-but-healthy release is
/// never mistaken for a wedged process; a minute is far beyond any plausible
/// pool teardown and still bounded.
///
/// Deliberately **not** floored by the configured handshake deadline: that
/// deadline is about spawn liveness — how long a fresh process may take to
/// answer a trivial frame — and an operator who tightened it to 5 s was
/// describing startup, not a `cudaFree` of a big pool.
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
    /// The variable a resolved device pin is written to:
    /// `CUDA_VISIBLE_DEVICES` on CUDA (and on hosts with no accelerator of
    /// their own), `HIP_VISIBLE_DEVICES` on ROCm — the two vocabularies are
    /// not interchangeable, so the pin *value* (`gpu::GpuInventory::resolve_pin`)
    /// and this name are one decision made in one place
    /// (`gpu::pin_env_var`, from the resolved accelerator; see
    /// docs/rocm-batch-calibration-parity.md, D2). This layer only writes
    /// what it is handed.
    pub pin_env_var: &'static str,
}

impl WorkerSpawnConfig {
    /// This config for a replica pinned to a **unified** board: the same
    /// thing plus `PANOPTIKON_UNIFIED_GPU=<that board's PCI address>`
    /// (DP-5), or the original untouched when the board is discrete.
    ///
    /// A per-replica question, which is why it is not part of the host-level
    /// worker env (`accelerator_env::worker_env`): on a dGPU+APU host one
    /// model's replicas can sit on both kinds of board, and this decides
    /// whether that worker's own memory arithmetic counts GTT. It travels
    /// through `env` rather than being written beside the pin in
    /// [`worker_command`] so that both spawners — a fresh load and a pool
    /// warm-up — get it from the one resolver that also produced the pin
    /// (`GpuInventory::unified_pin_bdf`), and so a model's own `env` still
    /// outranks it, as it outranks everything else here.
    ///
    /// The value is an address rather than a flag because the pin is a
    /// *belief* about where the replica will land, and the worker can check
    /// it against the board it actually came up on — see
    /// [`gpu::UNIFIED_GPU_ENV_VAR`](super::gpu::UNIFIED_GPU_ENV_VAR).
    pub fn for_unified_board(&self, bdf: Option<&str>) -> Cow<'_, Self> {
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

/// One instant's view of the worker's GPU memory, as reported on `load` and
/// `predict` responses (protocol doc, "Memory sensing"). Every field is
/// optional: a worker with no torch, no CUDA or no NVML reports what it can
/// and omits the rest, and absent always means "unknown" — never zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemorySample {
    pub free_mb: Option<u64>,
    pub total_mb: Option<u64>,
    /// Which driver `free_mb`/`total_mb` came from: `"nvml"`,
    /// `"amdgpu-sysfs"` (the ROCm whole-board counters) or `"torch"`
    /// (`mem_get_info`). They disagree by gigabytes on the same board — the
    /// two driver sources see the whole board, `mem_get_info` the calling
    /// context's view — so any consumer that differences two samples, or
    /// subtracts our own footprint from `free_mb` to price *other* processes,
    /// must first check that this matches. `None` when the worker could read
    /// none of them.
    pub free_source: Option<String>,
    /// Caching-allocator pool size (`torch.cuda.memory_reserved`).
    pub reserved_mb: Option<u64>,
    /// Live tensor bytes (`torch.cuda.memory_allocated`).
    pub allocated_mb: Option<u64>,
}

/// What the `load` response reports about the model's footprint. `base_mb`
/// is the worker's whole-*process* device footprint (context + workspaces +
/// weights), which is the currency the ledger charges residents in.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadReport {
    pub base_mb: Option<u64>,
    /// `"nvml"` | `"fdinfo"` (its ROCm twin) | `"free_delta"` |
    /// `"alloc_delta"` — provenance for the calibration profile, kept as the
    /// worker sent it.
    pub base_method: Option<String>,
    pub reserved_at_load_mb: Option<u64>,
    /// Negotiated load precision (`"fp16"`/`"bf16"`/`"fp32"`); part of the
    /// profile key. `None` when nothing negotiated one.
    pub dtype: Option<String>,
    /// The board the worker's CUDA device 0 *actually* resolved to, as the
    /// worker itself read it (`GPU-…`). This — not the spawn pin, which may
    /// be an index, absent, or a UUID CUDA reordered — is the authoritative
    /// ledger identity for step 1b.
    ///
    /// Always absent on a ROCm worker: torch renders a UUID there from the
    /// ASIC serial, but it is a third vocabulary matching neither KFD's nor
    /// amd-smi's and repeating across same-model consumer boards, so the
    /// worker suppresses it rather than emit an identity that can silently
    /// collide (docs/rocm-batch-calibration-parity.md, D3/F5). Those hosts
    /// are keyed by [`Self::gpu_bdf`].
    pub gpu_uuid: Option<String>,
    /// That board's name per torch. **Informational only** — nothing keys on
    /// it: the profile key uses the board name from the orchestrator's own
    /// inventory, so every profile this host writes is keyed by one string
    /// whatever each worker's torch calls the card
    /// (`VramLedger::register_worker`, and the protocol doc's `gpu_name`
    /// row).
    pub gpu_name: Option<String>,
    /// The board's PCI address (`dddd:bb:dd.0`), as the worker read it from
    /// `get_device_properties(0)`'s PCI fields — the one identity vocabulary
    /// the kernel, the driver and the HIP runtime all speak, and therefore
    /// the ROCm ledger join. Reported on CUDA hosts too (additive; the UUID
    /// still keys them). Absent on an older torch with no PCI fields and no
    /// usable fdinfo fallback.
    ///
    /// Which today means: **absent on the shipped CUDA build**, whose venv
    /// pins torch 2.7.1 — `_CudaDeviceProperties` grew the PCI fields in
    /// 2.8 — so this is live only on the `rocm` extra (torch 2.11) until
    /// that pin moves, and the identity chain it feeds is load-bearing on
    /// ROCm alone.
    pub gpu_bdf: Option<String>,
    /// That board's total VRAM in MiB, as **torch/HIP** reports it. The
    /// point is the provenance: registration cross-checks a BDF match
    /// against this, and it did not come from the sysfs file the inventory's
    /// own total was read from, so agreement is evidence rather than a file
    /// compared with itself (D3/F4).
    pub gpu_total_mb: Option<u64>,
    /// `torch.__version__`, part of the profile key and knowable only in the
    /// worker (the orchestrator does not know its venv's torch).
    pub torch_version: Option<String>,
    pub memory: Option<MemorySample>,
}

/// One GPU batch the worker actually ran, from a `predict` response (or an
/// `error` reply — a window that failed part-way still measured what ran).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchMeasurement {
    /// Inputs in the batch. Deliberately *not* cost-dimension units.
    pub items: Option<u64>,
    /// The batch's size in the model's declared cost dimension, as the
    /// packing harness priced it. Absent when the request carried no grant:
    /// without a declared dimension the worker has nothing to price in, and
    /// the ledger's fit only regresses against this.
    pub units: Option<u64>,
    pub reserved_before_mb: Option<u64>,
    pub peak_reserved_mb: Option<u64>,
    pub allocated_before_mb: Option<u64>,
    pub peak_allocated_mb: Option<u64>,
    /// Wall time of `instance.predict(batch)` only — the harness prices units
    /// outside the timed section so the throughput comparator sees GPU work.
    pub duration_ms: Option<f64>,
    /// The batch raised an out-of-memory condition: a negative sample for the
    /// ledger's deflation path.
    pub oom: bool,
    /// A pool-growing batch whose units/sec cratered against the previous
    /// one. On WDDM the driver's sysmem fallback turns over-admission into a
    /// silent throughput collapse instead of an OOM, so this is the synthetic
    /// negative sample that stands in for the exception that never fires.
    pub throughput_collapse: bool,
    //
    // The protocol's `trimmed` flag (protocol doc, "Measurements") is
    // deliberately **not** parsed into a field here. It marks the first
    // measurement of a window whose reactive shrink released the pool first,
    // and the ledger's answer to such a sample is to take it exactly as it
    // comes: a post-`empty_cache()` regrowth batch is a high-water batch, which
    // is precisely the kind the fit wants, and knowing it followed a release
    // would not change how it is priced (see the high-water branch of
    // `VramLedger::ingest_locked`, which spells out why). It stays on the wire
    // as operator-facing provenance — it explains an otherwise surprising
    // pool-growth spike in a log — and a field nothing reads would only invite
    // someone to give it a meaning the ledger does not have.
}

/// A telemetry reading plus when it was recorded. The ledger has to be able
/// to tell a fresh measurement from one taken before another process moved
/// on the same board, and 1a is where the clock has to start being kept —
/// timestamps cannot be reconstructed after the fact.
#[derive(Debug, Clone, PartialEq)]
pub struct Timestamped<T> {
    pub captured_at: Instant,
    pub value: T,
}

impl<T> Timestamped<T> {
    /// Stamp a reading with the current instant. `pub(super)` so the ledger's
    /// tests can build telemetry fixtures without a live worker.
    pub(super) fn now(value: T) -> Self {
        Self {
            captured_at: Instant::now(),
            value,
        }
    }
}

/// One recorded batch measurement: the reading, when it arrived, and a
/// per-worker sequence number.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchSample {
    /// Strictly increasing per worker, starting at 1, and never reused —
    /// including across ring evictions, which is what makes a gap
    /// detectable.
    pub seq: u64,
    pub captured_at: Instant,
    pub measurement: BatchMeasurement,
}

/// Everything a worker has told us about its memory, plus the GPU it was
/// pinned to at spawn. Shared by `Arc` with whoever owns the [`Worker`]
/// (the dispatcher task, after a load), because the budget arbiter is the
/// manager, not the dispatcher: the manager keeps a clone of this handle per
/// replica and reads it without disturbing anything.
///
/// Step 1a only records. The ledger that consumes these (grants, cost fits,
/// eviction) is step 1b.
#[derive(Debug, Clone, Default)]
pub struct WorkerTelemetry {
    /// Resolved device pin, in the vocabulary of the variable it was written
    /// to ([`WorkerSpawnConfig::pin_env_var`]): a `GPU-…` board UUID on a
    /// known CUDA inventory, a **HIP device index string** on a known ROCm
    /// one (`HIP_VISIBLE_DEVICES` accepts nothing else), else whatever the
    /// registry asked for, else `None` on hosts with no GPU inventory. Never
    /// changes after spawn.
    ///
    /// Operator-facing provenance only — it is surfaced on `/health` and
    /// nothing keys on it. The identity the ledger keys on is what the
    /// *worker* reported ([`LoadReport::gpu_uuid`], or [`LoadReport::gpu_bdf`]
    /// on ROCm), and the two can differ in form as well as in value (an index
    /// pin, an unknown inventory, a MIG instance, any ROCm host). The board
    /// key a replica's *pin* names is resolved separately, for the ledger's
    /// load reservation (`gpu::GpuInventory::resolve_board_key`).
    pub gpu: Option<String>,
    pub load: Option<Timestamped<LoadReport>>,
    /// Freshest sample, from whichever response carried one last.
    pub memory: Option<Timestamped<MemorySample>>,
    /// Bounded ring of the most recent measurements, oldest first.
    measurements: VecDeque<BatchSample>,
    recorded: u64,
}

impl WorkerTelemetry {
    /// Ring capacity.
    ///
    /// The ledger reads by watermark once per settled window, and one window
    /// with a deep grant can be *many* GPU batches — a `max-times-count` model
    /// bucketing a 64-item window into batches of two produces 32 measurements
    /// from a single frame, and the packing harness reports one entry per batch.
    /// 64 left almost no margin above that; 256 covers several such windows even
    /// if a settle is delayed, at a few tens of KB per replica. Overflow is not
    /// silent either way (`ingest_locked` names the gap), but the point is for it
    /// not to happen.
    pub const RING: usize = 256;

    /// Append the measurements of one `predict`, stamping each with the next
    /// sequence number. Every measurement gets its own entry: a request that
    /// the step-1b harness splits into several GPU batches is several fit
    /// samples, and collapsing them (as last-write-wins did) throws away
    /// exactly the varied batch sizes the cost model is fitted on.
    pub(super) fn record_measurements(&mut self, batches: Vec<BatchMeasurement>) {
        for measurement in batches {
            self.recorded += 1;
            self.measurements.push_back(BatchSample {
                seq: self.recorded,
                captured_at: Instant::now(),
                measurement,
            });
            while self.measurements.len() > Self::RING {
                self.measurements.pop_front();
            }
        }
    }

    /// The retained measurements, oldest first. **Non-draining**: several
    /// readers coexist (today `/health`, in 1b the ledger's cost fit), so
    /// nobody may consume on another's behalf.
    ///
    /// The intended 1b consumption pattern is a *watermark*, not a drain:
    /// remember the last `seq` fitted and take everything above it. That
    /// makes ring overflow visible instead of silent — if the oldest retained
    /// `seq` is already above the watermark, samples were dropped between
    /// reads (the reader is too slow, or the window rate too high) and the
    /// fit knows its sample set has a hole rather than assuming continuity.
    pub fn measurements(&self) -> impl DoubleEndedIterator<Item = &BatchSample> {
        self.measurements.iter()
    }

    /// How many measurements this worker has ever reported, including ones
    /// the ring has since evicted.
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
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<StderrTail>>,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
    _job_guard: JobGuard,
    /// Memory sensing shared with the manager (see [`WorkerTelemetry`]).
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
    /// The worker **stopped answering** — see [`Worker::is_dead`]. A strict
    /// subset of `dead`: every fatal error poisons the worker, but only some
    /// of them mean the process went away on its own.
    unreachable: bool,
}

/// Why a fatal teardown happened.
///
/// The distinction only exists because the ledger reads it: DP-2 turns a
/// mid-window *death* on a unified board into a synthetic negative sample
/// (docs/unified-memory-admission.md), and the whole point of that signal is
/// that an out-of-memory kill there arrives as a SIGKILL nothing in-process
/// can catch. A stream we tore down ourselves because the protocol state was
/// unrecoverable is not evidence about memory, so it must not settle as a
/// death.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FatalCause {
    /// The worker stopped answering: EOF on stdout, a broken pipe, an
    /// undecodable frame, a deadline that expired. These are the shapes a
    /// SIGKILL takes from this side of the pipe.
    ///
    /// Counting an expired **deadline** as a death is only safe because
    /// `predict` carries `deadline: None` (see [`Worker::predict`]): every
    /// request that can time out — handshake, configure, prewarm, load,
    /// trim, ping — runs outside any grant, so a timeout can never settle a
    /// window at all. Putting a deadline on `predict` would make a
    /// wedged-but-alive worker a memory negative on unified boards, and this
    /// variant would have to split.
    Unreachable,
    /// The exchange itself was fine — the worker was alive and talking — and
    /// we killed it because the stream can no longer be trusted: a request
    /// future was dropped mid-flight (what a user cancel produces), or a
    /// frame answered the wrong id.
    Desync,
}

/// The fully environment-shaped child command for one worker, per the
/// protocol's spawn contract (docs/inferio-worker-protocol.md,
/// "Environment"). Separate from [`Worker::spawn`] so the environment it
/// composes — in particular *which* visibility variable a device pin lands
/// in — is assertable without a Python interpreter on the box.
///
/// `device` is the pin `gpu::GpuInventory::resolve_pin` already resolved,
/// and [`WorkerSpawnConfig::pin_env_var`] is the variable that vocabulary
/// belongs to. Exactly one of them is ever written: `CUDA_VISIBLE_DEVICES`
/// is deliberately not also set on ROCm (it is a HIP alias, and AMD
/// documents setting both as unintended-behaviour territory), and the ROCm
/// worker env from `accelerator_env` sets no visibility variable at all, so
/// the two compose without overlapping.
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
        // Defense in depth for the stderr forwarder: keep Python's own
        // text streams UTF-8 regardless of the console code page
        // (cp1252 tracebacks on Windows). The Rust side still tolerates
        // arbitrary bytes — native libraries write to fd 2 directly.
        .env("PYTHONIOENCODING", "utf-8")
        // A set PYTHONHOME is never valid for a venv interpreter, and
        // AppImage-style launchers export one pointing into their mount,
        // which kills the child before main (missing 'encodings'). The
        // PYTHONPATH inherit below stays: it is a deliberate dev hook.
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
        // The same value under a name only we write, so the worker can tell
        // *our* placement from an operator's ambient visibility variable —
        // see `gpu::DEVICE_PIN_MARKER_ENV_VAR`.
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
    // An interactive Ctrl-C must reach the gateway alone; the shutdown
    // ladder (unload → terminate → kill) does the stopping. A worker hit
    // directly by the console signal dies before `unload` is sent and is
    // reported as an unexpected death.
    detach_from_console(&mut command);
    // And if the gateway dies with no cleanup at all (forced exit, OOM
    // kill), the kernel reaps the worker: job object on Windows,
    // PR_SET_PDEATHSIG on Unix.
    die_with_parent(&mut command);
    Ok(command)
}

/// One line when a worker's own env config touches a variable that decides
/// *where the model runs*, because that config is applied last and therefore
/// silently outranks what the orchestrator wrote.
///
/// Three shapes of collision, all worth the same warning. An entry for the
/// variable the pin was written to replaces the pin outright (or, via
/// `env_remove`, deletes it, leaving the worker whatever the gateway
/// inherited). An entry for a *different* visibility variable does not
/// overwrite anything, but the AMD stack still resolves the pair by its own
/// precedence — `HIP_VISIBLE_DEVICES` over its `CUDA_VISIBLE_DEVICES` alias,
/// both indexing into whatever `ROCR_VISIBLE_DEVICES` already filtered — so
/// the boards the worker ends up on are not necessarily the ones the pin
/// named. And an entry for [`DEVICE_ENV_VAR`] moves the device coherence
/// marker itself (docs/unified-memory-admission.md, backend C): it is what
/// `get_device()` honours and what the worker measures its memory currency
/// from, so a model config that sets or deletes it can put the impl on a
/// device the ledger is not pricing — on a CPU-priced host, the exact hole
/// the marker exists to close. Neither case is an error: an operator may well
/// mean it. It is simply the one env interaction whose symptom (a model
/// running on the wrong board, or on the CPU) points nowhere near its cause.
///
/// One line per spawn, listing every colliding variable, and only when there
/// is a collision to report. Called from [`Worker::spawn_configured`] rather
/// than from [`worker_command`], because that is the only place the *model's*
/// own entries are still distinguishable from the orchestrator's — see
/// [`colliding_device_variables`]. A pooled worker
/// ([`Worker::spawn`], `prewarm.rs`) is spawned by impl class with no model
/// env at all, so there is nothing there for this to report.
fn warn_on_visibility_overrides(cfg: &WorkerSpawnConfig, spec: &SpawnSpec, device: Option<&str>) {
    let overrides = colliding_device_variables(cfg, spec);
    if overrides.is_empty() {
        return;
    }
    // Two messages, because with no pin written the collision has nothing to
    // override: saying "the worker may not end up on the board it was pinned
    // to" there reads as an alarm about a pin that does not exist. The entry
    // is still worth one line — it is controlling device visibility on this
    // worker, and the orchestrator is not the thing that set it, which is
    // exactly the state that makes a later "why is this model on the wrong
    // card" impossible to trace.
    let message = if device.is_some() {
        "this worker's env config sets or removes a device-selection variable \
         (a GPU-visibility one, or the INFERIO_DEVICE coherence marker); it is \
         applied after the device pin, so an entry naming the pin's own \
         variable replaces (or deletes) the pin, and an entry naming another \
         one is resolved against it by the runtime's own precedence — either \
         way the worker may not end up on the board it was pinned to"
    } else {
        "this worker's env config sets or removes a device-selection variable \
         (a GPU-visibility one, or the INFERIO_DEVICE coherence marker) while \
         no device pin was written for this replica; the entry alone \
         therefore decides where the model runs, and the orchestrator's \
         ledger is pricing it against the board it believes rather than one \
         it placed it on"
    };
    tracing::warn!(
        variables = overrides.join(", "),
        pin_variable = cfg.pin_env_var,
        pin = device.unwrap_or("(none)"),
        "{message}"
    );
}

/// The device-selection variables this spawn's *model configuration* touches,
/// in a fixed order. Pure, so the decision is testable without a subscriber.
///
/// The two families are read from different places, and that asymmetry is the
/// whole point of splitting this out:
///
/// - the **visibility** variables are read from the merged spawn env
///   (`cfg`), which is where they have always been read from. Nothing the
///   orchestrator writes is one of them — `accelerator_env::worker_env`
///   emits HIP/MIOpen paths, the MPS watermarks and the device marker, and
///   `for_unified_board` emits a PCI address — so every hit there is the
///   model's anyway, and reading the merged view additionally catches one
///   arriving by some future route;
/// - [`DEVICE_ENV_VAR`](crate::accelerator_env::DEVICE_ENV_VAR) is read from
///   the **model spec alone**, because the orchestrator writes it itself on
///   every worker of a CPU-priced host. Filtering the merged env for it would
///   fire on every single spawn on such a host and blame the operator's model
///   config for the orchestrator's own entry — a warning that is not merely
///   noise but actively misleading, since it names the one variable whose
///   whole purpose is that the orchestrator controls it.
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
    /// (INFERIO_WORKER=1, PYTHONPATH prepend, the backend's device-visibility
    /// variable when a pin is given, PYTHONHOME removed, inherited env
    /// otherwise — see [`worker_command`]) — `device` is the *resolved* pin,
    /// a `GPU-…` board UUID on CUDA and a HIP device index on ROCm
    /// (`gpu.rs`); this layer only writes what it is handed —
    /// and perform the v2
    /// handshake — identity only (`impl_class` + the config's `impl_dirs`),
    /// no instantiation — within the handshake deadline. On any failure the
    /// child is killed and reaped and the error carries the worker
    /// traceback (from the `error` frame) or the stderr tail. The worker
    /// must be [`Worker::configure`]d (optionally after a
    /// [`Worker::prewarm`]) before `load`/`predict`.
    pub async fn spawn(
        cfg: &WorkerSpawnConfig,
        impl_class: &str,
        device: Option<String>,
    ) -> Result<Worker> {
        let mut command = worker_command(cfg, device.as_deref())?;
        let mut child = command.spawn().with_context(|| {
            format!(
                "failed to spawn inferio worker for impl class {impl_class} via {}",
                cfg.python.display()
            )
        })?;
        // Belt and braces on Windows: kill_on_drop only reaches the direct
        // child, the job object reaps the whole tree on any drop path.
        let job_guard = JobGuard::assign_tokio(&child);
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
        // Before the merge is out of sight: this is the only place the
        // model's own entries are still separable from the orchestrator's,
        // which one of the two families the warning covers depends on
        // ([`colliding_device_variables`]).
        warn_on_visibility_overrides(&spawn_cfg, spec, device.as_deref());
        let mut worker = Self::spawn(&spawn_cfg, &spec.impl_class, device)
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
    ///
    /// The response may carry the base measurement and a memory sample
    /// (protocol doc, "Memory sensing"); both are recorded in the shared
    /// [`WorkerTelemetry`] and acted on by nobody yet. A worker that reports
    /// nothing (no torch, a remote-API impl) leaves the telemetry untouched —
    /// which no longer includes CPU and MPS hosts, whose workers report in
    /// system-RAM and Metal-budget currency respectively
    /// (docs/unified-memory-admission.md).
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
                gpu_uuid = report.gpu_uuid.as_deref(),
                gpu_name = report.gpu_name.as_deref(),
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

    /// Shared memory-sensing handle for this worker, for the manager to keep
    /// alongside the replica after the dispatcher takes ownership.
    pub fn telemetry(&self) -> TelemetryHandle {
        Arc::clone(&self.telemetry)
    }

    /// Record the optional memory-sensing fields of a `predict` reply.
    ///
    /// Runs for `ok` **and** `error` frames: a window that failed part-way
    /// still measured whatever ran before the failure, and an out-of-memory
    /// batch is precisely the negative sample the ledger most needs.
    fn record_telemetry(&self, payload: &[(Value, Value)]) {
        let measurements = BatchMeasurement::parse_list(map_get(payload, "measurements"));
        let sample = MemorySample::parse(map_get(payload, "memory"));
        if sample.is_none() && measurements.is_empty() {
            return;
        }
        if let Ok(mut telemetry) = self.telemetry.lock() {
            if let Some(sample) = sample {
                telemetry.memory = Some(Timestamped::now(sample));
            }
            telemetry.record_measurements(measurements);
        }
    }

    /// Send `predict` with the given inputs and return one output per input,
    /// in order. No deadline in v1 (models take arbitrarily long); to cancel,
    /// drop the future and `kill()` the worker.
    ///
    /// `grant` is the window's memory grant (protocol doc, "Memory grants").
    /// With one, the worker's packing harness splits the inputs into several
    /// GPU batches within the budget and reports one measurement per batch;
    /// without one, the whole array goes to a single `instance.predict` call,
    /// which is the permanent compatibility path for `none`-class models and
    /// any host with no inventory at all (CPU and MPS hosts have admission
    /// boards of their own and do get grants —
    /// docs/unified-memory-admission.md). `fit` rides along only when the
    /// fitted cost model moved since the last frame to this worker.
    ///
    /// A slot may come back as [`WorkerOutput::Error`] — the worker's typed
    /// verdict on *that input alone* (protocol doc, "Per-item error slots").
    /// It is a normal, successful roundtrip: the count still has to match, so
    /// slot alignment downstream is unchanged, and only a malformed error
    /// object (or a wrong count) is fatal.
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
                // The reserved key with a body the protocol does not define is
                // a violation, exactly like a count mismatch: guessing a class
                // would let a broken worker fabricate an "undecodable media"
                // verdict, which the ledger would then persist.
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
                        Err(err) => {
                            // The exchange completed and the stream is in sync
                            // — an unconvertible output (non-finite float,
                            // nested bin/ext) is a per-request failure, not a
                            // supervision failure. Surface it as a WorkerError
                            // so the dispatcher applies its per-request
                            // fallback instead of killing a healthy worker and
                            // failing the whole queue.
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

    /// Send `trim` — release the caching allocator's unused pool
    /// (`empty_cache()`), keeping weights, live tensors and the CUDA context —
    /// and record the fresh memory sample the reply carries.
    ///
    /// The orchestrator sends this to an **idle** resident whose retained pool
    /// is squeezing a neighbour (docs/batch-calibration-design.md, "Trim for
    /// idle residents"). It is hygiene, not work: a worker that cannot trim
    /// (no torch, no live CUDA) replies `ok` with nothing, an older worker
    /// replies with a per-request `error` and stays alive, and both are fine.
    ///
    /// Recording the sample is the point of the round trip as far as the
    /// ledger is concerned: it is how the released slack stops being charged
    /// to a resident that will not run another window for a while.
    pub async fn trim(&mut self) -> Result<()> {
        let deadline = TRIM_DEADLINE;
        let payload = self
            .roundtrip("trim", Vec::new(), Some(deadline))
            .await
            .with_context(|| format!("trim failed for inferio worker {}", self.label))?;
        self.record_telemetry(&payload);
        Ok(())
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
                if let Some(task) = self.stderr_task.take() {
                    let _ = timeout(STDERR_JOIN_GRACE, task).await;
                }
                Ok(status)
            }
            Err(err) => {
                let tail = self.stderr_tail_snapshot();
                self.kill().await;
                Err(err.context(format!(
                    "graceful shutdown of inferio worker {name} failed; worker killed; stderr tail:\n{tail}"
                )))
            }
        }
    }

    /// Hard stop: terminate, wait `terminate_grace`, kill again if needed,
    /// and reap. Never fails; also the cancel path for in-flight predicts.
    pub async fn kill(mut self) {
        // Group first, then the child: descendants must not survive the
        // reap below turning the group kill into a no-op (Unix; Windows
        // relies on the job object dropping with self).
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
            bail!(
                "inferio worker {} is dead after a previous fatal error",
                self.label
            );
        }
        if self.in_flight {
            // Classified `Desync` even though the process may in fact be gone:
            // a stranded stream is discovered here rather than where it
            // happened, so if the OS killed the worker *after* a cancel
            // stranded it, this request sees the desync first and DP-2 misses
            // a real memory negative. A deliberate under-report — the
            // conservative direction is to lose a negative sample, not to
            // invent one out of a user pressing cancel.
            return Err(self
                .fatal_request(
                    request_type,
                    "a previous request future was dropped mid-flight; the stream is desynchronized"
                        .to_owned(),
                    FatalCause::Desync,
                )
                .await);
        }
        let id = self.next_id;
        self.next_id += 1;
        let mut frame = vec![
            (Value::from("type"), Value::from(request_type)),
            (Value::from("id"), Value::from(id)),
        ];
        frame.append(&mut fields);
        // Serialize fully before sending: an over-limit or unencodable frame
        // fails here without a byte hitting the stream — no protocol desync,
        // the worker is still serviceable. Surfaced as a WorkerError so the
        // dispatcher fails this request alone instead of killing the model.
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
        let cycle = async {
            send_bytes(stdin, &bytes).await?;
            read_frame(stdout).await
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
                // Telemetry on an error frame is advisory but valuable: the
                // batch that failed is the negative sample the ledger wants.
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

    /// [`Self::fatal`], plus the one thing the request type is needed for:
    /// naming a `trim` as the cause of a teardown before it happens.
    ///
    /// Every other request type is something a caller asked for, so a model
    /// dying on one has an obvious cause in the logs right above it. A trim is
    /// the exception — nobody asked for it, it is memory hygiene the
    /// orchestrator sent on its own initiative to an *idle* resident — so
    /// without this line an operator sees a model die with no request of
    /// theirs anywhere near it. Logged before the teardown so it precedes the
    /// death, whatever the reap does to ordering.
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

    /// Poison the worker after an unrecoverable failure: kill, reap, drain
    /// stderr, and build the error carrying exit status + stderr tail.
    async fn fatal(&mut self, why: String, cause: FatalCause) -> anyhow::Error {
        self.dead = true;
        self.unreachable = matches!(cause, FatalCause::Unreachable);
        self.in_flight = false;
        kill_process_group(&self.child);
        let _ = self.child.start_kill();
        let status = match timeout(FATAL_REAP_GRACE, self.child.wait()).await {
            Ok(Ok(status)) => status.to_string(),
            Ok(Err(err)) => format!("wait failed: {err}"),
            Err(_) => "still running (kill timed out)".to_owned(),
        };
        // The forwarder ends on stderr EOF once the child is gone; awaiting
        // it makes the tail snapshot complete instead of racy.
        if let Some(task) = self.stderr_task.take() {
            let _ = timeout(STDERR_JOIN_GRACE, task).await;
        }
        let tail = self.stderr_tail_snapshot();
        anyhow!(
            "inferio worker {} failed fatally: {why}; process status: {status}; stderr tail:\n{tail}",
            self.label
        )
    }

    /// Did this worker *die*, as opposed to being poisoned by a desync we
    /// killed it for — and if so, **claim** that fact.
    ///
    /// Deliberately narrower than "unusable": a worker whose stream we tore
    /// down ourselves ([`FatalCause::Desync`]) — the dropped-future path a
    /// user cancel produces, a frame answering the wrong id — is just as
    /// unusable, but it is the *dispatcher's* doing and says nothing about
    /// memory. The ledger blames a batch size for a death (DP-2's synthetic
    /// negative on unified boards), so only a worker that stopped answering
    /// on its own may settle a window as `WorkerDied`. An error that never
    /// reached [`Self::fatal`] at all — an oversized frame rejected by
    /// `encode_frame` before a byte hit the wire — leaves this `false`
    /// alongside a worker that is still perfectly alive.
    ///
    /// **Taking** rather than reading is the one-shot guard: one death may
    /// produce at most one negative sample. Today the dispatcher tears the
    /// model down on the first fatal error so a second granted window on a
    /// dead replica is unreachable, but nothing in the type system says so,
    /// and a future change that made it reachable would halve the model's
    /// ratchet anchor once per window instead of once per death. Every
    /// further call answers `false`, which settles as an abort — the correct
    /// reading, since the death was already accounted.
    pub(crate) fn take_death(&mut self) -> bool {
        std::mem::take(&mut self.unreachable)
    }

    fn stderr_tail_snapshot(&self) -> String {
        self.stderr
            .lock()
            .map(|tail| tail.snapshot())
            .unwrap_or_default()
    }

    /// Test hook: kill the child out from under the supervisor without
    /// touching any bookkeeping, simulating an external/OOM kill. Also used
    /// by the prewarm pool tests to kill a *parked* worker so the claim-time
    /// ping failure path is exercised.
    #[cfg(test)]
    pub(crate) async fn kill_child_externally_for_test(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    /// Test hook: leave the stream in the state a request future dropped
    /// mid-flight leaves it, without having to race a real cancel. The next
    /// request desynchronizes and kills a worker that is still alive.
    #[cfg(test)]
    pub(crate) fn strand_in_flight_for_test(&mut self) {
        self.in_flight = true;
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

/// Whole-MiB field: msgpack integers as sent, floats rounded (a worker that
/// ever switches to fractional MB must not silently read as absent).
/// Negative or non-finite values are treated as unknown.
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

impl MemorySample {
    /// `None` when the field is absent or not a map, or when every value in
    /// it is nil — an all-unknown sample carries no information.
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
        };
        (sample != Self::default()).then_some(sample)
    }
}

impl LoadReport {
    /// `None` when the response carried no memory-sensing fields at all,
    /// which is how an older worker (or one with no torch) answers.
    ///
    /// Visible to the rest of `inferio` (rather than private here) so the
    /// ledger's registration tests can start from a real msgpack payload
    /// instead of a hand-built struct: the provenance strings the ledger acts
    /// on — `free_source`, `base_method` — are carried opaquely from the
    /// worker to the board, and a round trip is the only test that covers the
    /// whole of that path.
    pub(super) fn parse(payload: &[(Value, Value)]) -> Option<Self> {
        let report = Self {
            base_mb: field_u64(payload, "base_mb"),
            base_method: field_string(payload, "base_method"),
            reserved_at_load_mb: field_u64(payload, "reserved_at_load_mb"),
            dtype: field_string(payload, "dtype"),
            gpu_uuid: field_string(payload, "gpu_uuid"),
            gpu_name: field_string(payload, "gpu_name"),
            gpu_bdf: field_string(payload, "gpu_bdf"),
            gpu_total_mb: field_u64(payload, "gpu_total_mb"),
            torch_version: field_string(payload, "torch_version"),
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
                    peak_reserved_mb: field_u64(map, "peak_reserved_mb"),
                    allocated_before_mb: field_u64(map, "allocated_before_mb"),
                    peak_allocated_mb: field_u64(map, "peak_allocated_mb"),
                    duration_ms: field_f64(map, "duration_ms"),
                    oom: field_bool(map, "oom"),
                    throughput_collapse: field_bool(map, "throughput_collapse"),
                })
            })
            .collect()
    }
}

/// Absent or non-boolean reads as `false`: these flags mean "the worker
/// observed this", so silence is never a signal.
fn field_bool(map: &[(Value, Value)], key: &str) -> bool {
    matches!(map_get(map, key), Some(Value::Boolean(true)))
}

/// The `grant` map on a `predict` request frame (protocol doc, "Memory
/// grants").
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
    ])
}

/// The `fit` map on a `predict` request frame; sent only when the fitted cost
/// model moved since the last frame to that worker.
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

/// Reads one msgpack output slot as a typed error (protocol doc, "Per-item
/// error slots"). `None` means an ordinary payload; `Some(Err(..))` means the
/// reserved key was there but the body is not a valid error object, which the
/// caller treats as a fatal protocol violation.
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

    /// Spawn config matching how the Python protocol tests drive the
    /// harness: repo venv python, cwd = repo root, PYTHONPATH=python (the
    /// subprocess must resolve the python/-layout package itself), NO_CUDNN so
    /// startup never probes CUDA paths (which would import torch), and the
    /// test fixture impl dir.
    pub(crate) fn test_spawn_config() -> WorkerSpawnConfig {
        let root = workspace_root();
        // PANOPTIKON_TEST_PYTHON overrides the repo-venv interpreter (any
        // python with msgpack works), e.g. running the suite under WSL
        // against a Windows checkout, whose .venv is a Windows venv.
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
            // The fixture impls echo `CUDA_VISIBLE_DEVICES`, which is also
            // what every non-ROCm host writes.
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

    /// D2's spawn half: a resolved pin goes into the visibility variable the
    /// backend dictates and into **no other one**. `CUDA_VISIBLE_DEVICES` is
    /// deliberately not also set on ROCm — it is a HIP alias, and setting
    /// both is documented unintended-behaviour territory — and the CUDA
    /// vocabulary must never reach HIP's variable, where a `GPU-…` string
    /// hides every board.
    ///
    /// Asserted against the composed command rather than a live worker, so
    /// it holds on any box, with or without an interpreter or a GPU.
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
        fn pin_env(cfg: &WorkerSpawnConfig, device: Option<&str>) -> Vec<(String, String)> {
            worker_command(cfg, device)
                .expect("the command composes")
                .as_std()
                .get_envs()
                .filter(|(key, _)| key.to_string_lossy().ends_with("VISIBLE_DEVICES"))
                .map(|(key, value)| {
                    (
                        key.to_string_lossy().into_owned(),
                        value.unwrap_or_default().to_string_lossy().into_owned(),
                    )
                })
                .collect()
        }

        assert_eq!(
            pin_env(&config(CUDA_PIN_ENV_VAR), Some("GPU-1a2b")),
            vec![("CUDA_VISIBLE_DEVICES".to_owned(), "GPU-1a2b".to_owned())],
            "a CUDA host writes the board UUID, and only that variable"
        );
        assert_eq!(
            pin_env(&config(HIP_PIN_ENV_VAR), Some("1")),
            vec![("HIP_VISIBLE_DEVICES".to_owned(), "1".to_owned())],
            "a ROCm host writes the HIP device index, and CUDA_VISIBLE_DEVICES \
             is deliberately left alone"
        );
        // No pin: no visibility variable at all, on either backend — the
        // worker inherits whatever the operator's environment says.
        assert!(pin_env(&config(CUDA_PIN_ENV_VAR), None).is_empty());
        assert!(pin_env(&config(HIP_PIN_ENV_VAR), None).is_empty());

        // DP-5 rides alongside the pin: a replica on a **unified** board is
        // told which board that is, because the worker has no inventory and
        // its own memory arithmetic has to count GTT there — and because the
        // pin is only a *belief* about where the replica lands, the value is
        // the board's address so the worker can check it against the board it
        // actually came up on. A replica on a discrete board sees no such
        // variable at all, which is what keeps its numbers byte-identical to
        // before this existed.
        let unified_env = |cfg: &WorkerSpawnConfig, bdf: Option<&str>| {
            let cfg = cfg.for_unified_board(bdf);
            worker_command(&cfg, Some("0"))
                .expect("the command composes")
                .as_std()
                .get_envs()
                .find(|(key, _)| key.to_string_lossy() == "PANOPTIKON_UNIFIED_GPU")
                .map(|(_, value)| value.unwrap_or_default().to_string_lossy().into_owned())
        };
        let rocm = config(HIP_PIN_ENV_VAR);
        assert_eq!(
            unified_env(&rocm, Some("0000:03:00.0")).as_deref(),
            Some("0000:03:00.0"),
            "the board's PCI address, not a flag"
        );
        // Lower-cased on the way out, because that is the spelling the worker
        // renders its own address in and the two are compared as strings.
        assert_eq!(
            unified_env(&rocm, Some("0000:0C:00.0")).as_deref(),
            Some("0000:0c:00.0")
        );
        assert_eq!(unified_env(&rocm, None), None);
        assert_eq!(unified_env(&config(CUDA_PIN_ENV_VAR), None), None);
        // The pin itself is untouched by either answer.
        assert_eq!(
            pin_env(&rocm.for_unified_board(Some("0000:03:00.0")), Some("0")),
            vec![("HIP_VISIBLE_DEVICES".to_owned(), "0".to_owned())]
        );
        // And the config a discrete replica spawns with is the caller's own,
        // not a copy — the flag is the only reason to clone one.
        assert!(matches!(
            rocm.for_unified_board(None),
            std::borrow::Cow::Borrowed(_)
        ));

        // The placement marker: the same pin under a name only we write, so
        // the worker's pinned-but-invisible tripwire can tell our placement
        // from an operator's ambient visibility variable (which looks
        // identical in the child's environment and means the opposite).
        let marker = |cfg: &WorkerSpawnConfig, device: Option<&str>| {
            worker_command(cfg, device)
                .expect("the command composes")
                .as_std()
                .get_envs()
                .find(|(key, _)| key.to_string_lossy() == "PANOPTIKON_DEVICE_PIN")
                .map(|(_, value)| value.unwrap_or_default().to_string_lossy().into_owned())
        };
        assert_eq!(marker(&rocm, Some("1")).as_deref(), Some("1"));
        assert_eq!(
            marker(&config(CUDA_PIN_ENV_VAR), Some("GPU-1a2b")).as_deref(),
            Some("GPU-1a2b")
        );
        assert_eq!(
            marker(&rocm, None),
            None,
            "no pin, no marker — an unpinned replica was placed by nobody"
        );
    }

    /// The device-override warning fires on the **model's** configuration and
    /// never on the orchestrator's own entries.
    ///
    /// The regression this pins down: `INFERIO_DEVICE` is written by
    /// `accelerator_env::worker_env` on every worker of a CPU-priced host and
    /// then merged with the model's env before the spawn, so a check against
    /// the merged view warned on *every single spawn* on such a host — and
    /// blamed the operator's model config for the one variable the
    /// orchestrator is supposed to own.
    #[test]
    fn the_device_override_warning_reads_the_models_own_env() {
        use crate::accelerator_env::DEVICE_ENV_VAR;
        use crate::inferio::gpu::CUDA_PIN_ENV_VAR;

        /// A spawn config as `http.rs` builds one, plus the model's entries
        /// merged on top exactly as `spawn_configured` merges them.
        fn merged(host_env: Vec<(String, String)>, spec: &SpawnSpec) -> WorkerSpawnConfig {
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
            cfg
        }

        let cpu_host = || vec![(DEVICE_ENV_VAR.to_owned(), "cpu".to_owned())];

        // (a) A CPU host and a model that configures nothing: the marker in
        // the merged env is ours, so there is nothing to report.
        let plain = spec("echo_test");
        assert_eq!(
            colliding_device_variables(&merged(cpu_host(), &plain), &plain),
            Vec::<&str>::new(),
            "the orchestrator's own marker must not warn about itself"
        );

        // (b) The model setting it *is* the collision, on any host — with or
        // without the orchestrator's entry underneath.
        let mut overriding = spec("echo_test");
        overriding
            .env
            .push((DEVICE_ENV_VAR.to_owned(), "cuda".to_owned()));
        assert_eq!(
            colliding_device_variables(&merged(cpu_host(), &overriding), &overriding),
            vec![DEVICE_ENV_VAR]
        );
        assert_eq!(
            colliding_device_variables(&merged(Vec::new(), &overriding), &overriding),
            vec![DEVICE_ENV_VAR]
        );
        // Deleting it is the same collision: the worker then probes the
        // hardware and can land off the board it is priced against.
        let mut deleting = spec("echo_test");
        deleting.env_remove.push(DEVICE_ENV_VAR.to_owned());
        assert_eq!(
            colliding_device_variables(&merged(cpu_host(), &deleting), &deleting),
            vec![DEVICE_ENV_VAR]
        );
        // Matched case-insensitively, like every other env comparison here.
        let mut lower = spec("echo_test");
        lower
            .env
            .push(("inferio_device".to_owned(), "cuda".to_owned()));
        assert_eq!(
            colliding_device_variables(&merged(cpu_host(), &lower), &lower),
            vec![DEVICE_ENV_VAR]
        );

        // (c) The visibility arm is unchanged: still read from the merged
        // env, still every variant, still in `VISIBILITY_VARS` order — and it
        // is never the orchestrator's, which writes no visibility variable
        // through `env` at all.
        let mut visible = spec("echo_test");
        visible
            .env
            .push(("CUDA_VISIBLE_DEVICES".to_owned(), "0".to_owned()));
        visible.env_remove.push("ROCR_VISIBLE_DEVICES".to_owned());
        assert_eq!(
            colliding_device_variables(&merged(cpu_host(), &visible), &visible),
            vec!["ROCR_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES"],
        );
        // Both families at once, marker last.
        let mut both = visible.clone();
        both.env
            .push((DEVICE_ENV_VAR.to_owned(), "cuda".to_owned()));
        assert_eq!(
            colliding_device_variables(&merged(cpu_host(), &both), &both),
            vec![
                "ROCR_VISIBLE_DEVICES",
                "CUDA_VISIBLE_DEVICES",
                DEVICE_ENV_VAR
            ]
        );
    }

    /// Full happy path against a real worker subprocess: spawn+handshake
    /// resolves the echo_test fixture impl, load succeeds, a mixed predict
    /// (JSON data with nested map/list/unicode + raw file bytes) returns
    /// ordered outputs with the right variants — the data input echoes back
    /// as `Json({"echo": data})` and the file input comes back as msgpack
    /// bin (`Bytes(b"echo:" + file)`) — and shutdown unloads gracefully with
    /// the worker exiting 0.
    #[tokio::test]
    async fn full_lifecycle_happy_path() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn_configured(&cfg, "test/echo", &spec("echo_test"), None)
            .await
            .expect("spawn + handshake");
        worker.load().await.expect("load ok");

        let data = json!({
            "text": "héllo wörld — 日本語",
            "nested": {"list": [1, 2.5, true, null, "внутри"]}
        });
        let inputs = [
            WorkerInput {
                data: Some(data.clone()),
                file: None,
            },
            WorkerInput {
                data: None,
                file: Some(vec![0x00, 0x01, 0xfe, 0xff]),
            },
        ];
        let outputs = worker
            .predict(&inputs, None, None)
            .await
            .expect("predict ok");
        assert_eq!(outputs.len(), 2, "one output per input, in order");
        assert_eq!(outputs[0], WorkerOutput::Json(json!({"echo": data})));
        assert_eq!(
            outputs[1],
            WorkerOutput::Bytes(b"echo:\x00\x01\xfe\xff".to_vec())
        );

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0), "worker exits 0 after unload");
    }

    /// `trim` round trip over the real protocol: the worker answers `ok`,
    /// stays configured and loaded, and serves the next `predict` normally.
    ///
    /// The fixture impls never import torch, so there is no pool to release
    /// and the reply carries no memory sample — which is the *point* of the
    /// assertion here: a trim on a worker that cannot trim is a plain success,
    /// not an error path, so the orchestrator never has to know in advance
    /// which residents are trimmable. The stream staying in sync (the predict
    /// below) is what proves the response frame was consumed correctly.
    #[tokio::test]
    async fn a_trim_is_answered_and_leaves_the_worker_serving() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn_configured(&cfg, "test/echo", &spec("echo_test"), None)
            .await
            .expect("spawn + handshake");
        worker.load().await.expect("load ok");

        worker.trim().await.expect("trim is answered with ok");
        worker.trim().await.expect("trim is idempotent");

        let inputs = [WorkerInput {
            data: Some(json!("still here")),
            file: None,
        }];
        let outputs = worker
            .predict(&inputs, None, None)
            .await
            .expect("the worker still serves predicts after a trim");
        assert_eq!(
            outputs[0],
            WorkerOutput::Json(json!({"echo": "still here"}))
        );
        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// End to end over the real protocol: a `predict` carrying a **grant**
    /// makes the worker's packing harness split the window into GPU batches
    /// inside the unit budget, report one measurement per batch (with the
    /// dimension-priced `units`), and still answer one output per input in
    /// the original order.
    ///
    /// The batchsize fixture reports the batch size it was actually handed,
    /// so this asserts the packing happened in the *worker* rather than being
    /// inferred from telemetry alone.
    #[tokio::test]
    async fn a_grant_makes_the_worker_pack_gpu_batches() {
        let cfg = test_spawn_config();
        let mut worker =
            Worker::spawn_configured(&cfg, "test/batch", &spec("batchsize_test"), None)
                .await
                .expect("spawn + handshake");
        worker.load().await.expect("load ok");
        let telemetry = worker.telemetry();

        let inputs: Vec<WorkerInput> = (0..5)
            .map(|index| WorkerInput {
                data: Some(json!(index)),
                file: None,
            })
            .collect();
        let grant = Grant {
            unit_budget: 2,
            mb: 1024,
            unit: super::super::cost::CostUnit::Item,
            aggregation: super::super::cost::CostAggregation::Count,
            user_cap_items: None,
        };
        let outputs = worker
            .predict(&inputs, Some(&grant), None)
            .await
            .expect("granted predict");
        assert_eq!(outputs.len(), 5, "one output per input");
        let sizes: Vec<u64> = outputs
            .iter()
            .map(|output| match output {
                WorkerOutput::Json(value) => value["batch"].as_u64().expect("batch"),
                other => panic!("unexpected output {other:?}"),
            })
            .collect();
        assert_eq!(
            sizes,
            vec![2, 2, 2, 2, 1],
            "the window was packed into batches of 2, 2, 1 inside the grant"
        );

        let measurements: Vec<BatchMeasurement> = telemetry
            .lock()
            .unwrap()
            .measurements()
            .map(|sample| sample.measurement.clone())
            .collect();
        assert_eq!(measurements.len(), 3, "one measurement per GPU batch");
        assert_eq!(
            measurements
                .iter()
                .map(|batch| batch.items)
                .collect::<Vec<_>>(),
            vec![Some(2), Some(2), Some(1)]
        );
        assert_eq!(
            measurements
                .iter()
                .map(|batch| batch.units)
                .collect::<Vec<_>>(),
            vec![Some(2), Some(2), Some(1)],
            "item/count prices one unit per item"
        );
        assert!(
            measurements.iter().all(|batch| !batch.oom
                && !batch.throughput_collapse
                && batch.duration_ms.is_some()),
            "clean batches, each individually timed: {measurements:?}"
        );

        // The user cap is an item-count constraint at pack time.
        let capped = Grant {
            unit_budget: 8,
            user_cap_items: Some(1),
            ..grant
        };
        let outputs = worker
            .predict(&inputs, Some(&capped), None)
            .await
            .expect("capped predict");
        assert!(
            outputs.iter().all(|output| match output {
                WorkerOutput::Json(value) => value["batch"].as_u64() == Some(1),
                _ => false,
            }),
            "a cap of 1 item overrides a unit budget of 8: {outputs:?}"
        );

        worker.shutdown().await.expect("graceful shutdown");
    }

    /// The negative-sample path end to end: a granted window whose *second*
    /// GPU batch OOMs fails as a whole (per-request semantics unchanged, the
    /// worker stays alive) but still reports both measurements on the error
    /// frame, the failing one flagged `oom`, and its message carries the
    /// whole-window OOM prefix the ledger classifies on.
    #[tokio::test]
    async fn a_granted_window_reports_its_oom_batch_on_the_error_frame() {
        let cfg = test_spawn_config();
        let mut worker =
            Worker::spawn_configured(&cfg, "test/oomsecond", &spec("oom_second_batch_test"), None)
                .await
                .expect("spawn + handshake");
        worker.load().await.expect("load ok");
        let telemetry = worker.telemetry();

        let inputs: Vec<WorkerInput> = (0..4)
            .map(|index| WorkerInput {
                data: Some(json!(index)),
                file: None,
            })
            .collect();
        let grant = Grant {
            unit_budget: 2,
            mb: 1024,
            unit: super::super::cost::CostUnit::Item,
            aggregation: super::super::cost::CostAggregation::Count,
            user_cap_items: None,
        };
        let err = worker
            .predict(&inputs, Some(&grant), None)
            .await
            .expect_err("the second batch OOMs, so the window fails");
        let worker_err = err
            .downcast_ref::<WorkerError>()
            .expect("a per-request failure: the worker survives it");
        assert!(
            worker_err.message.contains("INFERENCE_OOM_WINDOW:"),
            "the whole-window OOM signal: {}",
            worker_err.message
        );
        assert!(
            super::super::ledger::message_reports_oom(&worker_err.message),
            "and the ledger classifies it as a negative sample"
        );

        let measurements: Vec<BatchMeasurement> = telemetry
            .lock()
            .unwrap()
            .measurements()
            .map(|sample| sample.measurement.clone())
            .collect();
        assert_eq!(
            measurements.len(),
            2,
            "telemetry is recorded from error frames too: {measurements:?}"
        );
        assert!(!measurements[0].oom, "the batch that ran was clean");
        assert_eq!(measurements[0].units, Some(2), "and priced");
        assert!(measurements[1].oom, "the batch that failed is the negative");
        assert_eq!(
            measurements[1].units, None,
            "a failed batch is never priced: its peaks stop where the call gave \
             up, so pricing it would feed the fit an under-stated cost"
        );

        // The worker survived: a smaller window still succeeds... except this
        // fixture is now permanently past its first batch, so assert only
        // liveness, which is the contract that matters.
        worker
            .ping()
            .await
            .expect("the worker is still serviceable");
        worker.shutdown().await.expect("graceful shutdown");
    }

    /// A handshake naming an impl_class no fixture module provides must fail
    /// the spawn with an error that carries the worker's own message and
    /// traceback (from the `error` frame), downcastable to WorkerError; the
    /// child process is killed/reaped by the spawn error path (the test
    /// completing without a hang is the observable half of that).
    #[tokio::test]
    async fn spawn_unknown_impl_class_surfaces_worker_traceback() {
        let cfg = test_spawn_config();
        let err =
            match Worker::spawn_configured(&cfg, "test/missing", &spec("does_not_exist"), None)
                .await
            {
                Ok(_) => panic!("handshake with an unknown impl_class must fail"),
                Err(err) => err,
            };
        let text = format!("{err:#}");
        assert!(
            text.contains("does_not_exist"),
            "error should carry the worker's message: {text}"
        );
        let worker_err = err
            .downcast_ref::<WorkerError>()
            .expect("handshake error frame maps to WorkerError");
        assert!(
            worker_err.traceback.contains("LookupError"),
            "traceback text from the worker is preserved: {}",
            worker_err.traceback
        );
    }

    /// predict before load is the protocol's sanity-check error: the worker
    /// replies with an `error` frame (surfaced as WorkerError mentioning
    /// load) but stays alive and serviceable — a follow-up ping succeeds on
    /// the same worker.
    #[tokio::test]
    async fn predict_before_load_is_worker_error_and_worker_survives() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn_configured(&cfg, "test/echo", &spec("echo_test"), None)
            .await
            .expect("spawn + handshake");

        let err = worker
            .predict(
                &[WorkerInput {
                    data: Some(json!("x")),
                    file: None,
                }],
                None,
                None,
            )
            .await
            .expect_err("predict before load must fail");
        let worker_err = err
            .downcast_ref::<WorkerError>()
            .expect("per-request failure maps to WorkerError");
        assert!(
            worker_err.message.contains("load"),
            "message explains the missing load: {}",
            worker_err.message
        );

        worker.ping().await.expect("worker is still serviceable");
        // Cleanup: unload without a prior load still exits 0 (harness skips
        // instance.unload() when not loaded).
        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// A worker killed externally mid-session (simulating an OOM kill or a
    /// crash) must fail the next predict promptly with a fatal error carrying
    /// the process exit status — not a WorkerError, and never a hang, even
    /// though predict has no deadline (EOF on stdout is the wakeup).
    #[tokio::test]
    async fn externally_killed_worker_fails_next_predict_without_hanging() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn_configured(&cfg, "test/echo", &spec("echo_test"), None)
            .await
            .expect("spawn + handshake");
        worker.load().await.expect("load ok");

        worker.kill_child_externally_for_test().await;

        let err = worker
            .predict(
                &[WorkerInput {
                    data: Some(json!(1)),
                    file: None,
                }],
                None,
                None,
            )
            .await
            .expect_err("predict against a dead worker must fail");
        assert!(
            err.downcast_ref::<WorkerError>().is_none(),
            "process death is a fatal supervision error, not a worker error frame"
        );
        let text = format!("{err:#}");
        assert!(
            text.contains("process status"),
            "error reports the exit status and stderr tail: {text}"
        );

        // The worker is poisoned: further requests fail fast.
        let err = worker.ping().await.expect_err("dead worker stays dead");
        assert!(format!("{err:#}").contains("dead"));
    }

    /// stdout hygiene end-to-end: the printing_test fixture print()s during
    /// load/predict/unload, which lands on stderr in the worker (fd 1 is
    /// dup2'd to stderr before impl code runs) — so every protocol frame
    /// still parses, predict returns its real outputs, shutdown is a clean
    /// exit 0, and all three printed strings (load/predict/unload) were
    /// captured on stderr rather than lost or leaked onto stdout.
    #[tokio::test]
    async fn stdout_hygiene_survives_printing_impl() {
        let cfg = test_spawn_config();
        let mut worker =
            Worker::spawn_configured(&cfg, "test/printer", &spec("printing_test"), None)
                .await
                .expect("spawn + handshake");
        worker.load().await.expect("load ok despite print()");

        let inputs = [
            WorkerInput {
                data: Some(json!(1)),
                file: None,
            },
            WorkerInput {
                data: Some(json!(2)),
                file: None,
            },
        ];
        let outputs = worker
            .predict(&inputs, None, None)
            .await
            .expect("predict ok");
        assert_eq!(
            outputs,
            vec![
                WorkerOutput::Json(json!({"printed": true})),
                WorkerOutput::Json(json!({"printed": true})),
            ]
        );

        // Keep a handle on the shared tail: shutdown() consumes the worker,
        // and the unload print only arrives during the graceful stop.
        let tail = Arc::clone(&worker.stderr);
        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
        let text = tail.lock().unwrap().snapshot();
        for expected in [
            "garbage on load stdout",
            "garbage on predict stdout",
            "garbage on unload stdout",
        ] {
            assert!(
                text.contains(expected),
                "stderr tail should contain {expected:?}:\n{text}"
            );
        }
    }

    /// The stderr forwarder must survive arbitrary bytes: badbytes_test
    /// writes raw invalid UTF-8 and a >64 KiB \r-only run (tqdm-style, no
    /// newlines) straight to fd 2 during predict. With the old lines()-based
    /// forwarder the first invalid byte killed the task, the pipe filled,
    /// and the deadline-less predict hung the worker forever; now both
    /// predicts succeed and the tail contains the lossily-decoded marker
    /// written *after* the bad bytes — proof the forwarder kept reading.
    #[tokio::test]
    async fn stderr_forwarder_survives_invalid_utf8_and_cr_only_runs() {
        let cfg = test_spawn_config();
        let mut worker =
            Worker::spawn_configured(&cfg, "test/badbytes", &spec("badbytes_test"), None)
                .await
                .expect("spawn + handshake");
        worker.load().await.expect("load ok");

        let input = [WorkerInput {
            data: Some(json!(1)),
            file: None,
        }];
        let outputs = worker
            .predict(&input, None, None)
            .await
            .expect("predict succeeds despite stderr garbage");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"bad": true}))]);

        // A follow-up predict proves the worker (and its stderr pipe) is
        // still fully serviceable.
        let outputs = worker
            .predict(&input, None, None)
            .await
            .expect("second predict");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"bad": true}))]);

        // The forwarder drains asynchronously; poll for the marker line
        // that the fixture writes after the invalid bytes and the \r run.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let tail = worker.stderr_tail_snapshot();
            if tail.contains("marker-after-bad-bytes") {
                assert!(!tail.is_empty(), "stderr tail must be non-empty");
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("stderr tail never captured the post-garbage marker: {tail:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// The msgpack half of the error-slot decoder, in isolation: only a map
    /// carrying the reserved key is a slot error, a malformed body is
    /// reported as a violation rather than guessed at, and ordinary payloads
    /// (including maps with other keys) are left alone.
    #[test]
    fn error_slot_from_rmpv_accepts_only_the_documented_shape() {
        let slot = Value::Map(vec![(
            Value::from(ERROR_SLOT_KEY),
            Value::Map(vec![
                (Value::from("class"), Value::from("input")),
                (Value::from("message"), Value::from("Unreadable image")),
            ]),
        )]);
        assert_eq!(
            error_slot_from_rmpv(&slot),
            Some(Ok(SlotError {
                class: super::super::slot_error::SlotErrorClass::Input,
                message: "Unreadable image".to_owned(),
            }))
        );

        for payload in [
            Value::Binary(vec![1, 2]),
            Value::from("text"),
            Value::Map(vec![(Value::from("tags"), Value::from("a"))]),
        ] {
            assert_eq!(error_slot_from_rmpv(&payload), None, "{payload}");
        }

        for malformed in [
            Value::Map(vec![(Value::from(ERROR_SLOT_KEY), Value::from("boom"))]),
            Value::Map(vec![(
                Value::from(ERROR_SLOT_KEY),
                Value::Map(vec![(Value::from("class"), Value::from("blocked"))]),
            )]),
            Value::Map(vec![(
                Value::from(ERROR_SLOT_KEY),
                Value::Map(vec![(Value::from("class"), Value::from("input"))]),
            )]),
        ] {
            assert!(
                matches!(error_slot_from_rmpv(&malformed), Some(Err(_))),
                "{malformed} must be rejected"
            );
        }
    }

    /// Per-item error slots end to end against a real worker: a batch mixing
    /// two typed failures with healthy JSON and binary outputs comes back
    /// with every slot in its input's position (alignment is the whole point
    /// — a shifted slot would blame the wrong file), and the worker is still
    /// serviceable afterwards, because an error slot is a *successful*
    /// roundtrip, not a failure.
    #[tokio::test]
    async fn error_slots_decode_and_stay_aligned_with_healthy_outputs() {
        let cfg = test_spawn_config();
        let mut worker =
            Worker::spawn_configured(&cfg, "test/errorslot", &spec("errorslot_test"), None)
                .await
                .expect("spawn + handshake");
        worker.load().await.expect("load ok");

        let inputs = [
            WorkerInput {
                data: Some(json!("first")),
                file: None,
            },
            WorkerInput {
                data: Some(json!("bad")),
                file: None,
            },
            WorkerInput {
                data: None,
                file: Some(b"payload".to_vec()),
            },
            WorkerInput {
                data: Some(json!("flaky")),
                file: None,
            },
        ];
        let outputs = worker
            .predict(&inputs, None, None)
            .await
            .expect("predict ok");
        assert_eq!(outputs.len(), inputs.len(), "one slot per input");
        assert_eq!(outputs[0], WorkerOutput::Json(json!({"ok": "first"})));
        assert_eq!(
            outputs[1],
            WorkerOutput::Error(SlotError {
                class: super::super::slot_error::SlotErrorClass::Input,
                message: "Unreadable image: truncated".to_owned(),
            })
        );
        assert_eq!(outputs[2], WorkerOutput::Bytes(b"bytes:payload".to_vec()));
        assert_eq!(
            outputs[3],
            WorkerOutput::Error(SlotError {
                class: super::super::slot_error::SlotErrorClass::Transient,
                message: "try again".to_owned(),
            })
        );

        // Nothing about the worker changed: it keeps serving.
        let outputs = worker
            .predict(
                &[WorkerInput {
                    data: Some(json!("again")),
                    file: None,
                }],
                None,
                None,
            )
            .await
            .expect("worker is still serviceable");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": "again"}))]);
        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// A slot carrying the reserved key with a body the protocol does not
    /// define is a protocol violation, exactly like a count mismatch: the
    /// worker is killed and poisoned rather than the class being guessed —
    /// guessing would let a broken worker fabricate an "undecodable media"
    /// verdict that the ledger then persists.
    #[tokio::test]
    async fn a_malformed_error_slot_kills_the_worker() {
        let cfg = test_spawn_config();
        let mut worker =
            Worker::spawn_configured(&cfg, "test/errorslot", &spec("errorslot_test"), None)
                .await
                .expect("spawn + handshake");
        worker.load().await.expect("load ok");

        let err = worker
            .predict(
                &[WorkerInput {
                    data: Some(json!("malformed")),
                    file: None,
                }],
                None,
                None,
            )
            .await
            .expect_err("a malformed error slot must fail the predict");
        let text = format!("{err:#}");
        assert!(
            text.contains("malformed error slot"),
            "the error names the violation: {text}"
        );
        assert!(
            err.downcast_ref::<WorkerError>().is_none(),
            "a protocol violation is fatal, not a per-request worker error"
        );
        let err = worker.ping().await.expect_err("the worker is poisoned");
        assert!(format!("{err:#}").contains("dead"));
    }

    /// Non-finite floats and binary/ext nested inside a JSON-like output
    /// have no JSON form: rmpv_to_json must report an error (never silently
    /// coerce — the Python side would equally fail to JSON-encode them),
    /// while ordinary finite floats convert cleanly.
    #[test]
    fn rmpv_to_json_rejects_nonfinite_and_nested_binary() {
        assert!(rmpv_to_json(&Value::F64(f64::NAN)).is_err());
        assert!(rmpv_to_json(&Value::F64(f64::INFINITY)).is_err());
        assert!(rmpv_to_json(&Value::F32(f32::NEG_INFINITY)).is_err());
        assert!(rmpv_to_json(&Value::Array(vec![Value::Binary(vec![1, 2])])).is_err());
        assert!(rmpv_to_json(&Value::Ext(7, vec![0])).is_err());
        assert_eq!(rmpv_to_json(&Value::F64(1.5)).unwrap(), json!(1.5));
    }

    /// Data fidelity: a JSON value exercising nested unicode strings,
    /// positive/negative/large integers, floats, booleans, null, lists, and
    /// maps survives the JSON → msgpack → Python → msgpack → JSON round trip
    /// through the echo impl with exact serde_json equality (ints stay ints,
    /// floats stay floats, unicode is untouched).
    #[tokio::test]
    async fn predict_data_round_trips_with_exact_json_fidelity() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn_configured(&cfg, "test/echo", &spec("echo_test"), None)
            .await
            .expect("spawn + handshake");
        worker.load().await.expect("load ok");

        let data = json!({
            "unicode": "こんにちは — ünïcode ✓ emoji 🦀",
            "int": 42,
            "negative": -7,
            "big": 9_007_199_254_740_993_i64,
            "float": 3.25,
            "bool": true,
            "null": null,
            "list": [1, "two", 3.5, false, null, {"nested": "map"}],
            "map": {"inner": {"deep": ["リスト", 2.0, -1]}}
        });
        let outputs = worker
            .predict(
                &[WorkerInput {
                    data: Some(data.clone()),
                    file: None,
                }],
                None,
                None,
            )
            .await
            .expect("predict ok");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"echo": data}))]);

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// The v2 pooled flow end to end: spawn by impl class (handshake only,
    /// no instantiation), prewarm (runs the prepare_test fixture's
    /// prepare() classmethod — proven by its stderr marker), park (ping,
    /// like the orchestrator would before claiming a parked worker), then
    /// configure + load + predict — the fixture reports the module flag
    /// prepare() set, so `{"prepared": true}` proves the prewarm actually
    /// ran in-process before the model was bound. Graceful shutdown exits 0.
    #[tokio::test]
    async fn prewarm_park_configure_load_happy_path() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn(&cfg, "prepare_test", None)
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
            .predict(
                &[WorkerInput {
                    data: Some(json!(1)),
                    file: None,
                }],
                None,
                None,
            )
            .await
            .expect("predict ok");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"prepared": true}))]);

        // The prepare() stderr marker was forwarded (drains asynchronously;
        // poll briefly).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if worker
                .stderr_tail_snapshot()
                .contains("prepare_test-prepare-marker")
            {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "prepare() marker never reached the stderr tail: {:?}",
                    worker.stderr_tail_snapshot()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0), "worker exits 0 after unload");
    }

    /// unload is valid in every state: a parked prewarmed worker (spawn +
    /// prewarm, never configured — no instance exists) is dismissed via the
    /// same graceful ladder and exits 0.
    #[tokio::test]
    async fn parked_worker_unloads_gracefully() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn(&cfg, "echo_test", None)
            .await
            .expect("spawn + identity handshake");
        worker.prewarm().await.expect("prewarm (no prepare) is ok");
        let status = worker
            .shutdown()
            .await
            .expect("graceful shutdown while parked");
        assert_eq!(status.code(), Some(0), "parked worker exits 0 on unload");
    }

    /// configure errors are per-request: a config kwarg the impl __init__
    /// rejects yields a WorkerError (downcastable, with the Python
    /// traceback) and must NOT poison the worker — a follow-up configure
    /// with good kwargs succeeds on the same process and the worker serves
    /// normally.
    #[tokio::test]
    async fn failed_configure_does_not_poison_worker() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn(&cfg, "prepare_test", None)
            .await
            .expect("spawn + identity handshake");

        // predict before configure is the state-machine sanity error.
        let err = worker
            .predict(
                &[WorkerInput {
                    data: Some(json!(1)),
                    file: None,
                }],
                None,
                None,
            )
            .await
            .expect_err("predict before configure must fail");
        let worker_err = err
            .downcast_ref::<WorkerError>()
            .expect("per-request failure maps to WorkerError");
        assert!(
            worker_err.message.contains("configure"),
            "message explains the missing configure: {}",
            worker_err.message
        );

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
        worker.load().await.expect("first instance is intact");

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// Version-mismatch kill: a stale harness that answers the handshake
    /// with protocol_version 1 (the fake_v1_harness package, shadowing the
    /// real one via a PYTHONPATH prepend) must fail the spawn with a fatal
    /// error naming the version — and the child is killed/reaped by the
    /// fatal path (the fake lingers on stdin, so the test finishing without
    /// a hang is the observable half of the kill).
    #[tokio::test]
    async fn version_mismatch_kills_worker() {
        let mut cfg = test_spawn_config();
        cfg.pythonpath.insert(
            0,
            workspace_root().join("python/tests/inferio_worker/fake_v1_harness"),
        );
        let err = match Worker::spawn(&cfg, "echo_test", None).await {
            Ok(_) => panic!("a v1 handshake echo must be rejected"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(
            text.contains("protocol_version") && text.contains("expected 2"),
            "error names the version mismatch: {text}"
        );
        assert!(
            err.downcast_ref::<WorkerError>().is_none(),
            "version mismatch is a fatal supervision error, not a worker error frame"
        );
    }

    fn measurement(items: u64) -> BatchMeasurement {
        BatchMeasurement {
            items: Some(items),
            ..BatchMeasurement::default()
        }
    }

    /// Every measurement is kept, not just the last response's: the cost fit
    /// in step 1b is fitted on a *set* of (units, peak) points, so the
    /// telemetry is a bounded ring with per-worker sequence numbers rather
    /// than a last-write-wins slot. Sequence numbers keep climbing across
    /// ring evictions, which is what lets a watermark reader notice it lost
    /// samples instead of assuming continuity.
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

        // One response carrying several GPU batches (the step-1b packing
        // harness) contributes one sample each.
        telemetry.record_measurements(vec![measurement(10), measurement(11)]);
        assert_eq!(telemetry.recorded_measurements(), 5);
        assert_eq!(telemetry.measurements().next_back().map(|s| s.seq), Some(5));

        // Overflow: the ring is bounded, the counter is not.
        for items in 0..WorkerTelemetry::RING as u64 {
            telemetry.record_measurements(vec![measurement(items)]);
        }
        assert_eq!(telemetry.measurements().count(), WorkerTelemetry::RING);
        assert_eq!(
            telemetry.recorded_measurements(),
            5 + WorkerTelemetry::RING as u64
        );
        let oldest = telemetry.measurements().next().expect("ring is full").seq;
        assert!(
            oldest > 1,
            "the oldest retained seq moved past 1, so a stale watermark is \
             detectable as a gap"
        );
        assert_eq!(
            telemetry.measurements().next_back().map(|s| s.seq),
            Some(telemetry.recorded_measurements()),
            "the newest sample's seq is the running count"
        );
    }

    /// The worker is a separate process and its response map is untrusted
    /// input: a wrong type, a negative count or a nil where a map belongs
    /// must read as "unknown", never as a wrong number and never as a
    /// protocol failure (the load itself succeeded).
    #[test]
    fn load_report_parse_tolerates_wrong_types() {
        let garbage = vec![
            (Value::from("base_mb"), Value::from("4321")),
            (Value::from("base_method"), Value::from(7i64)),
            (Value::from("reserved_at_load_mb"), Value::from(-5i64)),
            (Value::from("dtype"), Value::from(2.5f64)),
            (Value::from("gpu_uuid"), Value::Nil),
            (Value::from("memory"), Value::Nil),
        ];
        assert_eq!(
            LoadReport::parse(&garbage),
            None,
            "nothing usable is the same as an older worker reporting nothing"
        );

        // memory as an array (not a map) is ignored while the good fields
        // around it survive.
        let mixed = vec![
            (Value::from("base_mb"), Value::from(4321u64)),
            (Value::from("base_method"), Value::from("nvml")),
            (
                Value::from("memory"),
                Value::Array(vec![Value::from(1u64), Value::from(2u64)]),
            ),
            (Value::from("gpu_uuid"), Value::from("GPU-1a2b")),
            (Value::from("gpu_name"), Value::from(42i64)),
            (Value::from("gpu_bdf"), Value::from(3i64)),
            (Value::from("gpu_total_mb"), Value::from("24576")),
            (Value::from("torch_version"), Value::from("2.7.1+cu128")),
        ];
        let report = LoadReport::parse(&mixed).expect("the good fields are kept");
        assert_eq!(report.base_mb, Some(4321));
        assert_eq!(report.base_method.as_deref(), Some("nvml"));
        assert_eq!(report.memory, None);
        assert_eq!(report.gpu_uuid.as_deref(), Some("GPU-1a2b"));
        assert_eq!(report.gpu_name, None, "a non-string name is unknown");
        assert_eq!(report.gpu_bdf, None, "a non-string address is unknown");
        assert_eq!(
            report.gpu_total_mb, None,
            "a stringified total is unknown, never a parsed number: the \
             registration cross-check must not admit a board on a guess"
        );
        assert_eq!(report.torch_version.as_deref(), Some("2.7.1+cu128"));

        // A whole-MiB float (a worker that ever switches to fractional MB)
        // rounds rather than reading as absent; a negative one is unknown.
        let floats = vec![
            (Value::from("base_mb"), Value::from(1536.4f64)),
            (Value::from("reserved_at_load_mb"), Value::from(-1.0f64)),
        ];
        let report = LoadReport::parse(&floats).expect("float base is usable");
        assert_eq!(report.base_mb, Some(1536));
        assert_eq!(report.reserved_at_load_mb, None);
    }

    /// A ROCm worker's load report: no `gpu_uuid` at all (torch renders a
    /// third-vocabulary one on HIP and the worker suppresses it), a PCI
    /// address instead, and the board's total as torch sees it — the pair
    /// the ledger keys and cross-checks a ROCm replica by.
    #[test]
    fn load_report_carries_the_rocm_identity_fields() {
        let rocm = vec![
            (Value::from("base_mb"), Value::from(2048u64)),
            (Value::from("base_method"), Value::from("alloc_delta")),
            (Value::from("gpu_bdf"), Value::from("0000:03:00.0")),
            (Value::from("gpu_total_mb"), Value::from(24_560u64)),
            (
                Value::from("gpu_name"),
                Value::from("AMD Radeon RX 7900 XTX"),
            ),
            (Value::from("torch_version"), Value::from("2.11.0+rocm7.2")),
        ];
        let report = LoadReport::parse(&rocm).expect("a report with no uuid is a report");
        assert_eq!(report.gpu_uuid, None);
        assert_eq!(report.gpu_bdf.as_deref(), Some("0000:03:00.0"));
        assert_eq!(report.gpu_total_mb, Some(24_560));
        assert_eq!(report.torch_version.as_deref(), Some("2.11.0+rocm7.2"));

        // The new fields alone are enough to make a report: a worker that
        // could measure nothing else still has an identity to register with.
        let identity_only = vec![(Value::from("gpu_bdf"), Value::from("0000:0c:00.0"))];
        assert_eq!(
            LoadReport::parse(&identity_only).map(|report| report.gpu_bdf),
            Some(Some("0000:0c:00.0".to_owned()))
        );
    }

    /// The measurement array is per-batch data from the same untrusted
    /// source: entries that are not maps are skipped, not fatal, and a
    /// non-array field yields no measurements at all.
    #[test]
    fn measurement_list_skips_non_map_entries() {
        let list = Value::Array(vec![
            Value::Nil,
            Value::from(7i64),
            Value::Map(vec![
                (Value::from("items"), Value::from(8u64)),
                (Value::from("peak_reserved_mb"), Value::from(1200u64)),
                (Value::from("duration_ms"), Value::from(12.5f64)),
            ]),
            Value::Array(vec![Value::from("items")]),
            Value::Map(vec![(Value::from("items"), Value::from("eight"))]),
        ]);
        let measurements = BatchMeasurement::parse_list(Some(&list));
        assert_eq!(measurements.len(), 2, "only the two maps became entries");
        assert_eq!(measurements[0].items, Some(8));
        assert_eq!(measurements[0].peak_reserved_mb, Some(1200));
        assert_eq!(measurements[0].duration_ms, Some(12.5));
        assert_eq!(
            measurements[1],
            BatchMeasurement::default(),
            "a map with only unusable values is an all-unknown measurement"
        );

        assert!(BatchMeasurement::parse_list(None).is_empty());
        assert!(BatchMeasurement::parse_list(Some(&Value::Nil)).is_empty());
        assert!(
            BatchMeasurement::parse_list(Some(&Value::from("measurements"))).is_empty(),
            "a non-array field is no measurements, not a panic"
        );
    }
}
