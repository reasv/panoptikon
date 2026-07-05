//! Worker-process supervision: the orchestrator side of
//! `docs/inferio-worker-protocol.md` (v1).
//!
//! A [`Worker`] wraps one `python -m inferio_worker` child. Frames are 4-byte
//! little-endian u32 length + one msgpack map over the child's stdin/stdout;
//! stderr lines are forwarded to `tracing` with a per-worker prefix and a
//! bounded tail is kept for error reports. The protocol allows exactly one
//! outstanding request per worker, which is enforced structurally: every
//! request method takes `&mut self`.
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
//!   kill-on-close Job Object on Windows and tokio `kill_on_drop`, so no
//!   drop path can leak a worker tree.

use std::collections::VecDeque;
use std::env;
use std::fmt;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use rmpv::Value;
use serde_json::Value as JsonValue;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

use super::registry::SpawnSpec;
use crate::process_tree::JobGuard;

/// Protocol version this orchestrator speaks; workers answering anything
/// else in the handshake are killed.
const PROTOCOL_VERSION: u64 = 1;

/// Max frame size (512 MiB). Either side treats a larger declared length as
/// a fatal protocol error.
const MAX_FRAME_BYTES: usize = 0x2000_0000;

/// Bounds for the per-worker stderr tail ring buffer kept for error reports.
const STDERR_TAIL_MAX_LINES: usize = 50;
const STDERR_TAIL_MAX_BYTES: usize = 8 * 1024;

/// How long to wait for the stderr forwarder task to drain after the child
/// exited (it ends on EOF; this only bounds scheduling latency).
const STDERR_JOIN_GRACE: Duration = Duration::from_secs(1);

/// How long a fatal path waits for the killed child to be reaped.
const FATAL_REAP_GRACE: Duration = Duration::from_secs(5);

/// Lifecycle deadlines from the protocol doc ("Lifecycle and timeouts").
/// `predict` deliberately has no deadline in v1: models take arbitrarily
/// long, and cancellation means killing the worker.
#[derive(Debug, Clone, Copy)]
pub struct WorkerDeadlines {
    /// Spawn → handshake response (default 30 s). Also used for `ping`,
    /// whose whole point is bounded liveness checking.
    pub handshake: Duration,
    /// `load` response deadline; long because it covers heavy dependency
    /// imports plus weight loading (default 600 s).
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
    pub cwd: Option<PathBuf>,
    pub deadlines: WorkerDeadlines,
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
/// numpy etc.), anything else is converted to JSON.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerOutput {
    Bytes(Vec<u8>),
    Json(JsonValue),
}

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
    inference_id: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<StderrTail>>,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
    _job_guard: JobGuard,
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
}

impl Worker {
    /// Spawn `python -m inferio_worker` per the protocol's spawn contract
    /// (INFERIO_WORKER=1, PYTHONPATH prepend, CUDA_VISIBLE_DEVICES when a
    /// device pin is given, inherited env otherwise) and perform the
    /// handshake within the handshake deadline. On any failure the child is
    /// killed and reaped and the error carries the worker traceback (from
    /// the `error` frame) or the stderr tail.
    pub async fn spawn(
        cfg: &WorkerSpawnConfig,
        inference_id: &str,
        spec: &SpawnSpec,
        device: Option<String>,
    ) -> Result<Worker> {
        let mut command = Command::new(&cfg.python);
        command
            .arg("-m")
            .arg("inferio_worker")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env("INFERIO_WORKER", "1");
        if !cfg.pythonpath.is_empty() {
            let mut entries = cfg.pythonpath.clone();
            if let Some(existing) = env::var_os("PYTHONPATH") {
                entries.extend(env::split_paths(&existing));
            }
            let joined = env::join_paths(&entries)
                .context("PYTHONPATH entries contain the path separator")?;
            command.env("PYTHONPATH", joined);
        }
        if let Some(device) = device.as_deref() {
            command.env("CUDA_VISIBLE_DEVICES", device);
        }
        for (key, value) in &cfg.env {
            command.env(key, value);
        }
        if let Some(cwd) = &cfg.cwd {
            command.current_dir(cwd);
        }

        let mut child = command.spawn().with_context(|| {
            format!(
                "failed to spawn inferio worker for {inference_id} via {}",
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
            inference_id.to_owned(),
            Arc::clone(&tail),
        ));

        let mut worker = Worker {
            inference_id: inference_id.to_owned(),
            child,
            stdin,
            stdout,
            stderr: tail,
            stderr_task: Some(stderr_task),
            _job_guard: job_guard,
            deadlines: cfg.deadlines,
            next_id: 1,
            in_flight: false,
            dead: false,
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
            (Value::from("inference_id"), Value::from(inference_id)),
            (
                Value::from("impl_class"),
                Value::from(spec.impl_class.as_str()),
            ),
            (Value::from("config"), json_to_rmpv(&spec.config_kwargs)),
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
                return Err(
                    err.context(format!("inferio worker handshake failed for {inference_id}"))
                );
            }
        };
        let version = map_get(&payload, "protocol_version").and_then(Value::as_u64);
        if version != Some(PROTOCOL_VERSION) {
            return Err(worker
                .fatal(format!(
                    "worker answered handshake with protocol_version {version:?}, expected {PROTOCOL_VERSION}"
                ))
                .await);
        }
        Ok(worker)
    }

    /// Send `load` and await `ok` within the load deadline. Idempotent on
    /// the worker side (the impl's own load() guard).
    pub async fn load(&mut self) -> Result<()> {
        let deadline = self.deadlines.load;
        self.roundtrip("load", Vec::new(), Some(deadline))
            .await
            .map(|_| ())
            .with_context(|| format!("load failed for inferio worker {}", self.inference_id))
    }

    /// Send `predict` with the given inputs and return one output per input,
    /// in order. No deadline in v1 (models take arbitrarily long); to cancel,
    /// drop the future and `kill()` the worker.
    pub async fn predict(&mut self, inputs: &[WorkerInput]) -> Result<Vec<WorkerOutput>> {
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
        let mut payload = self
            .roundtrip(
                "predict",
                vec![(Value::from("inputs"), Value::Array(entries))],
                None,
            )
            .await
            .with_context(|| format!("predict failed for inferio worker {}", self.inference_id))?;
        let outputs = match take_field(&mut payload, "outputs") {
            Some(Value::Array(outputs)) => outputs,
            other => {
                return Err(self
                    .fatal(format!(
                        "predict ok frame without a valid outputs array: {other:?}"
                    ))
                    .await);
            }
        };
        // A count mismatch would silently mis-route outputs once the
        // dispatcher splits batches per request; the worker cannot be
        // trusted after it.
        if outputs.len() != inputs.len() {
            return Err(self
                .fatal(format!(
                    "worker returned {} outputs for {} inputs",
                    outputs.len(),
                    inputs.len()
                ))
                .await);
        }
        outputs
            .into_iter()
            .enumerate()
            .map(|(index, output)| match output {
                Value::Binary(bytes) => Ok(WorkerOutput::Bytes(bytes)),
                other => rmpv_to_json(&other).map(WorkerOutput::Json).with_context(|| {
                    format!("predict output {index} is not representable as JSON")
                }),
            })
            .collect()
    }

    /// Liveness check: send `ping`, await `ok`. Bounded by the handshake
    /// deadline (an unbounded liveness probe would be useless).
    pub async fn ping(&mut self) -> Result<()> {
        let deadline = self.deadlines.handshake;
        self.roundtrip("ping", Vec::new(), Some(deadline))
            .await
            .map(|_| ())
            .with_context(|| format!("ping failed for inferio worker {}", self.inference_id))
    }

    /// Graceful stop ladder: `unload` → await `ok` + process exit within
    /// `unload_grace`, else terminate, wait `terminate_grace`, then kill.
    /// The child is always reaped. Returns the exit status on the graceful
    /// path (the harness exits 0 after unload).
    pub async fn shutdown(mut self) -> Result<ExitStatus> {
        let name = self.inference_id.clone();
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
                bail!("unload was not acknowledged (type {resp_type:?}, id {resp_id:?}): {message}");
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
                self.inference_id
            );
        }
        if self.in_flight {
            return Err(self
                .fatal(
                    "a previous request future was dropped mid-flight; the stream is desynchronized"
                        .to_owned(),
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
        // fails here without a byte hitting the stream, so it is a plain
        // error, not a protocol desync.
        let bytes = encode_frame(&Value::Map(frame))?;

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
                    .fatal(format!("{request_type} request failed: {err:#}"))
                    .await);
            }
        };
        let map = match value {
            Value::Map(map) => map,
            other => {
                return Err(self
                    .fatal(format!("response frame is not a map: {other}"))
                    .await);
            }
        };
        let resp_type = map_get(&map, "type")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let resp_id = map_get(&map, "id").and_then(Value::as_u64);
        if resp_id != Some(id) {
            return Err(self
                .fatal(format!(
                    "response id {resp_id:?} does not match request id {id}"
                ))
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
                .fatal(format!("unexpected response frame type {other:?}"))
                .await),
        }
    }

    /// Poison the worker after an unrecoverable failure: kill, reap, drain
    /// stderr, and build the error carrying exit status + stderr tail.
    async fn fatal(&mut self, why: String) -> anyhow::Error {
        self.dead = true;
        self.in_flight = false;
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
            self.inference_id
        )
    }

    fn stderr_tail_snapshot(&self) -> String {
        self.stderr
            .lock()
            .map(|tail| tail.snapshot())
            .unwrap_or_default()
    }

    /// Test hook: kill the child out from under the supervisor without
    /// touching any bookkeeping, simulating an external/OOM kill.
    #[cfg(test)]
    async fn kill_child_externally_for_test(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

/// Forward worker stderr lines to tracing and the shared tail buffer.
/// Ends on EOF (worker exit) or a read error.
async fn forward_stderr(stderr: ChildStderr, inference_id: String, tail: Arc<Mutex<StderrTail>>) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::info!(worker = %inference_id, "{line}");
        if let Ok(mut tail) = tail.lock() {
            tail.push(line);
        }
    }
}

/// Serialize one frame payload, enforcing the 512 MiB limit before any byte
/// is written (a failure here never corrupts the stream).
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

fn map_get<'a>(map: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .map(|(_, v)| v)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    /// Repo root = CARGO_MANIFEST_DIR/.. (the gateway crate lives one level
    /// below the workspace root).
    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    /// Spawn config matching how the Python protocol tests drive the
    /// harness: repo venv python, cwd = repo root, PYTHONPATH=src (the
    /// subprocess must resolve the src/-layout package itself), NO_CUDNN so
    /// startup never probes CUDA paths (which would import torch), and the
    /// test fixture impl dir.
    fn test_spawn_config() -> WorkerSpawnConfig {
        let root = workspace_root();
        let python = if cfg!(windows) {
            root.join(".venv/Scripts/python.exe")
        } else {
            root.join(".venv/bin/python")
        };
        if !python.is_file() {
            panic!(
                "inferio worker tests need the repo venv interpreter at {} — create the dev venv first",
                python.display()
            );
        }
        WorkerSpawnConfig {
            python,
            impl_dirs: vec![root.join("tests/inferio_worker/fixture_impls")],
            pythonpath: vec![root.join("src")],
            env: vec![("NO_CUDNN".to_owned(), "true".to_owned())],
            cwd: Some(root),
            deadlines: WorkerDeadlines::default(),
        }
    }

    fn spec(impl_class: &str) -> SpawnSpec {
        SpawnSpec {
            impl_class: impl_class.to_owned(),
            config_kwargs: json!({}),
        }
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
        let mut worker = Worker::spawn(&cfg, "test/echo", &spec("echo_test"), None)
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
        let outputs = worker.predict(&inputs).await.expect("predict ok");
        assert_eq!(outputs.len(), 2, "one output per input, in order");
        assert_eq!(outputs[0], WorkerOutput::Json(json!({"echo": data})));
        assert_eq!(
            outputs[1],
            WorkerOutput::Bytes(b"echo:\x00\x01\xfe\xff".to_vec())
        );

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0), "worker exits 0 after unload");
    }

    /// A handshake naming an impl_class no fixture module provides must fail
    /// the spawn with an error that carries the worker's own message and
    /// traceback (from the `error` frame), downcastable to WorkerError; the
    /// child process is killed/reaped by the spawn error path (the test
    /// completing without a hang is the observable half of that).
    #[tokio::test]
    async fn spawn_unknown_impl_class_surfaces_worker_traceback() {
        let cfg = test_spawn_config();
        let err = match Worker::spawn(&cfg, "test/missing", &spec("does_not_exist"), None).await {
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
        let mut worker = Worker::spawn(&cfg, "test/echo", &spec("echo_test"), None)
            .await
            .expect("spawn + handshake");

        let err = worker
            .predict(&[WorkerInput {
                data: Some(json!("x")),
                file: None,
            }])
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
        let mut worker = Worker::spawn(&cfg, "test/echo", &spec("echo_test"), None)
            .await
            .expect("spawn + handshake");
        worker.load().await.expect("load ok");

        worker.kill_child_externally_for_test().await;

        let err = worker
            .predict(&[WorkerInput {
                data: Some(json!(1)),
                file: None,
            }])
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
    /// still parses, predict returns its real outputs, and shutdown is a
    /// clean exit 0.
    #[tokio::test]
    async fn stdout_hygiene_survives_printing_impl() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn(&cfg, "test/printer", &spec("printing_test"), None)
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
        let outputs = worker.predict(&inputs).await.expect("predict ok");
        assert_eq!(
            outputs,
            vec![
                WorkerOutput::Json(json!({"printed": true})),
                WorkerOutput::Json(json!({"printed": true})),
            ]
        );

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }

    /// Data fidelity: a JSON value exercising nested unicode strings,
    /// positive/negative/large integers, floats, booleans, null, lists, and
    /// maps survives the JSON → msgpack → Python → msgpack → JSON round trip
    /// through the echo impl with exact serde_json equality (ints stay ints,
    /// floats stay floats, unicode is untouched).
    #[tokio::test]
    async fn predict_data_round_trips_with_exact_json_fidelity() {
        let cfg = test_spawn_config();
        let mut worker = Worker::spawn(&cfg, "test/echo", &spec("echo_test"), None)
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
            .predict(&[WorkerInput {
                data: Some(data.clone()),
                file: None,
            }])
            .await
            .expect("predict ok");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"echo": data}))]);

        let status = worker.shutdown().await.expect("graceful shutdown");
        assert_eq!(status.code(), Some(0));
    }
}
