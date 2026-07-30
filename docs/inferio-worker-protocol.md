# Inferio Worker Protocol v2

v2 (2026-07-05): handshake carries worker *identity* only (impl class); the
new `configure` message binds a concrete model's config and instantiates.
This is what makes prewarm pools keyable by impl class — a warm worker can
be claimed for any model of its family. `prewarm` is no longer reserved: it
runs the impl's optional `prepare()` classmethod (heavy dependency imports,
no weights). Version bumped so a stale harness fails loudly at handshake.

2026-07-30: optional memory-sensing fields on the `load` and `predict`
responses (see "Memory sensing" below). Strictly additive — an older worker
simply omits them and an older orchestrator ignores them — so the version
stays 2.

Contract between the Rust orchestrator (parent) and a Python inference worker
(child process). Companion to `inferio-rust-orchestrator-design.md` §4.
Both implementations MUST follow this document exactly; change the document
first if the protocol needs to change.

## Transport

- The worker is spawned as a direct child; frames flow over the worker's
  **stdin** (orchestrator → worker) and **stdout** (worker → orchestrator).
- **stderr** is free-form UTF-8 logs; the orchestrator forwards each line to
  its own logging with a per-worker prefix. Tracebacks belong on stderr (and
  in `error` frames), never on stdout.
- **Frame** = 4-byte **little-endian u32** payload length, then exactly that
  many bytes of a single msgpack-encoded map. Max frame size 512 MiB
  (`0x2000_0000`); either side treats a larger declared length as a fatal
  protocol error (kill/exit).
- One request at a time: the orchestrator MUST NOT send a new request before
  receiving the response to the previous one (`ping` included). The worker
  processes frames strictly sequentially. Request ids exist for sanity
  checking only.

### stdout hygiene (critical)

Python libraries print. Before importing anything beyond the stdlib, the
harness MUST:

1. `real_stdout = os.fdopen(os.dup(1), "wb")` — grab the protocol channel;
2. `os.dup2(2, 1)` — redirect fd 1 to stderr so any stray native/library
   write to stdout becomes a log line instead of frame corruption;
3. rebind `sys.stdout = sys.stderr` for the same reason at the Python level;
4. put `real_stdout` (and stdin) into binary, unbuffered mode; on Windows,
   `msvcrt.setmode(fd, os.O_BINARY)` on both.

## Messages

Every frame is a msgpack map with at least:

- `"type"`: string (message type)
- `"id"`: unsigned integer — request id, chosen by the orchestrator,
  strictly increasing per worker. Responses echo the request's `id`.

Unknown map keys MUST be ignored (forward compatibility). Unknown request
`type` → worker replies `error` with `"unsupported"` in the message and
continues (does not exit).

### Orchestrator → worker (requests)

| type | fields | semantics |
|---|---|---|
| `handshake` | `protocol_version` (int, =2), `impl_class` (str — value matched against impl `name()`), `impl_dirs` (array of str — absolute paths searched for impl modules, in order) | First frame after spawn. Worker locates the impl class and replies `ok`. Does NOT instantiate and does NOT load weights — the worker's identity is the impl class, so a prewarmed worker can later be claimed for any model of that family. |
| `prewarm` | — | Calls the impl's optional `prepare()` classmethod (imports heavy deps, must not load weights or touch the GPU allocator; default absent = no-op → plain `ok`). Allowed only between `handshake` and `configure`; idempotent. Errors are per-request (`error` reply, worker stays alive and still usable — a failed prepare just means the later `load` pays the imports). |
| `configure` | `inference_id` (str, "group/name", for logs), `config` (map — resolved kwargs for the impl `__init__`) | Instantiates `impl_class(**config)`. Exactly once per worker, before `load`; a second `configure`, or `load`/`predict` before it, is a per-request `error`. |
| `load` | — | Calls `instance.load()`. Requires prior `configure`. Idempotent (repeat → `ok` without reloading, matching today's `InferenceModel.load()` guard semantics). |
| `predict` | `inputs`: array of maps `{ "data": <any msgpack value or nil>, "file": <bin or nil> }` | Calls `instance.predict(...)` with the inputs converted to `PredictionInput(data, file)` equivalents, in order. Requires a prior successful `load`; without one, reply `error` (the orchestrator always loads first — this is a sanity check, not a feature). |
| `unload` | — | Calls `instance.unload()` if an instance was configured+loaded, replies `ok`, flushes, then exits 0. Valid in every state (a parked prewarmed worker is dismissed the same way). |
| `ping` | — | Liveness. Reply `ok`. |

Normal spawn flow: `handshake` → `configure` → `load`. Pooled flow:
`handshake` → `prewarm` → (parked, possibly for hours) → `configure` →
`load`. The orchestrator SHOULD `ping` a parked worker before claiming it
(it may have died while parked) and fall back to a fresh spawn.

### Worker → orchestrator (responses)

| type | fields | semantics |
|---|---|---|
| `ok` | request-specific payload (below) | Success for the echoed `id`. |
| `error` | `message` (str), `traceback` (str, may be empty) | Failure for the echoed `id`. The worker stays alive and serviceable after an `error` (a failed predict/load must not require a respawn) — except a failed `handshake`, after which it exits non-zero. |

`ok` payloads:

- `handshake` → `protocol_version` (int): the version the worker speaks.
  v2 workers echo 2; the orchestrator kills workers that answer anything else.
- `configure`, `prewarm`, `unload`, `ping` → no extra fields.
- `load` → no required fields, plus the optional memory-sensing fields below.
- `predict` → `outputs`: array, one entry per input, in order. Each entry is
  either msgpack `bin` (bytes output, e.g. serialized numpy) or any other
  msgpack value (JSON-like output). This mirrors what impl `predict()`
  returns today: `bytes` stay bytes, everything else is data. Plus the
  optional memory-sensing fields below.

### Memory sensing (optional response fields)

Added for batch calibration (`docs/batch-calibration-design.md`): the worker
is the only side that has torch, the allocator statistics and the decoded
input, so it reports what it measured and the orchestrator does all the
sizing. Every field here is **optional and additive** — a worker with no
torch, no CUDA, or no NVML omits what it cannot measure, and the
orchestrator treats every absent field as "unknown" (never an error). The
protocol version is therefore unchanged (unknown map keys are ignored by
both sides).

Sensing is strictly **passive**: the worker never imports torch and never
initializes a CUDA context in order to answer. If the impl did not import
torch, or imported it without ever initializing CUDA, there is nothing of
ours on the device to measure and the fields are simply absent — a sensing
harness that created a context would itself allocate the 300–600 MB it is
supposed to be measuring, on hosts that were never going to use the GPU.

All memory values are **whole MiB** (`1024 * 1024` bytes), non-negative,
sent as msgpack integers. Durations are milliseconds as a float.

A **memory sample** map (used by several fields below) carries the device's
state at one instant, each key present but possibly nil:

| key | meaning |
|---|---|
| `free_mb` | driver-reported free memory on the worker's GPU |
| `total_mb` | driver-reported total memory on the worker's GPU |
| `free_source` | which driver told us: `"nvml"` or `"torch"` (`mem_get_info`). Absent/nil when neither could answer |
| `reserved_mb` | torch caching-allocator pool size (`memory_reserved`) |
| `allocated_mb` | live tensor bytes (`memory_allocated`) |

`free_mb`/`total_mb` always come from **one** source, named by `free_source`.
The two do not agree — NVML sees the whole board, `mem_get_info` the calling
context's view (measured 3.4 GB apart on the dev box) — so a consumer that
differences two samples, or subtracts our own footprint from `free_mb` to
estimate what other processes hold, must only compare readings whose
`free_source` matches. NVML is preferred whenever it is usable, because it
also answers *before* this process has a CUDA context, which `mem_get_info`
cannot do without creating one.

`load` `ok` may additionally carry:

| field | meaning |
|---|---|
| `base_mb` | the worker's whole-**process** device footprint after load (CUDA context + workspaces + weights), not just its allocator footprint. Absent — never zero — when the process demonstrably never touched the GPU (no torch, CPU/MPS host, remote API, or a torch-importing engine like CTranslate2 whose VRAM the allocator never sees) |
| `base_method` | how `base_mb` was obtained: `"nvml"` (own-PID `usedGpuMemory`), `"free_delta"` (driver free-memory delta across the load), or `"alloc_delta"` (allocator peak delta plus a fixed context allowance — the floor). Always names the term that actually produced the reported number |
| `reserved_at_load_mb` | allocator pool size right after load; the orchestrator prices later pool growth against this |
| `dtype` | the negotiated load precision, one of `"fp16"`, `"bf16"`, `"fp32"` (part of the calibration profile key). Absent when the impl does not negotiate one (CPU impls, remote APIs) |
| `gpu_uuid` | the board the worker's CUDA device 0 actually resolved to, in nvidia-smi/NVML form (`"GPU-<uuid>"`). This — not the `CUDA_VISIBLE_DEVICES` value the orchestrator spawned it with — is the authoritative GPU identity for the calibration ledger. Absent when the worker has no initialized CUDA device |
| `gpu_name` | that board's marketing name as torch reports it (e.g. `"NVIDIA GeForce RTX 5090"`), part of the calibration profile key |
| `torch_version` | `torch.__version__` (e.g. `"2.7.1+cu128"`), part of the calibration profile key. Only the worker knows which torch its venv holds. Absent when the impl never imported torch |
| `memory` | a memory sample taken right after load |

`predict` `ok` may additionally carry:

| field | meaning |
|---|---|
| `memory` | a memory sample taken right after the call |
| `measurements` | array of per-batch measurement maps, in execution order |

A measurement map describes one GPU batch the worker actually ran:

| key | meaning |
|---|---|
| `items` | number of inputs in the batch — a plain count, which is all the worker can know without the packing harness |
| `reserved_before_mb` / `peak_reserved_mb` | allocator pool size before the batch and its high-water mark during it |
| `allocated_before_mb` / `peak_allocated_mb` | live-tensor bytes before the batch and their high-water mark during it |
| `duration_ms` | wall time of the batch |

`items` is deliberately *not* cost-dimension units: decoded pixels, tokens
and audio-seconds are known only where the inputs are decoded, so a `units`
key (priced in the model's declared cost dimension) arrives together with the
packing harness in step 1b of `docs/batch-calibration-design.md`. Until then
a consumer that needs units must derive them itself; `items` is exact for the
`item`/`count` class and nothing else.

The worker resets torch's peak counters before each measured batch, so peaks
are per-batch rather than cumulative. A worker that packs one request frame
into several GPU batches reports one entry per batch; today it reports a
single entry covering the whole `predict` call, which is why the field is an
array from the start.

## Lifecycle and timeouts (orchestrator side)

- Spawn → send `handshake` → response deadline (config, default 30 s).
  Timeout/exit/garbage → kill, surface stderr tail in the load error.
- `load` deadline is long (weights + dep imports; config, default 600 s).
- `predict` has no fixed deadline in v1 (arbitrary models); cancellation =
  kill the worker (it is the model — there is nothing softer to cancel).
- Graceful stop: `unload` → wait (config, default 10 s) for `ok` + process
  exit; on timeout the worker is hard-terminated immediately, reaped within
  the terminate grace (config, default 5 s), and killed again as a last
  resort if the reap times out. There is no separate soft-terminate
  (SIGTERM) step between the unload grace and the hard kill: tokio offers no
  cross-platform soft terminate and Windows (the primary platform) has no
  SIGTERM equivalent — the `unload` exchange *is* the soft step. The whole
  tree is additionally under a kill-on-close Job Object on Windows.
- Unexpected worker exit at any point: all pending/queued requests for that
  model fail with the stderr tail; the model is marked unloaded.

## Environment (spawn contract)

The orchestrator sets for every worker:

- `CUDA_VISIBLE_DEVICES` — when device pinning is active (absent = default).
- `INFERIO_WORKER=1` — marker for impl code that wants to know.
- `PYTHONIOENCODING=utf-8` — keeps worker stderr valid UTF-8 (defense in
  depth; the orchestrator's stderr forwarder tolerates arbitrary bytes from
  native code regardless).
- Inherited: `DATA_FOLDER`, proxy vars, PATH. Nothing else is promised.

The worker runs `python -m inferio_worker` with no arguments; everything it
needs arrives in the handshake.
