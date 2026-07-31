# Inferio Worker Protocol v2

v2 (2026-07-05): handshake carries worker *identity* only (impl class); the
new `configure` message binds a concrete model's config and instantiates.
This is what makes prewarm pools keyable by impl class — a warm worker can
be claimed for any model of its family. `prewarm` is no longer reserved: it
runs the impl's optional `prepare()` classmethod (heavy dependency imports,
no weights). Version bumped so a stale harness fails loudly at handshake.

2026-07-30: optional memory-sensing fields on the `load` and `predict`
responses (see "Memory sensing" below), and the optional `grant`/`fit` fields
on the `predict` *request* that drive the worker's packing harness (see
"Memory grants" below). Strictly additive in both directions — an older
worker ignores the request fields and omits the response ones, an older
orchestrator sends neither and ignores whatever comes back — so the version
stays 2.

2026-07-30 (step 2): the `trim` request type (see "Trim" below) and the
optional `trimmed` flag on a measurement. Also additive: an older worker
answers an unknown `type` with a per-request `error` and stays alive, which
is exactly how a trim to a worker that cannot do one should behave, so the
version still stays 2.

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
| `predict` | `inputs`: array of maps `{ "data": <any msgpack value or nil>, "file": <bin or nil> }`, plus the optional `grant`/`fit` maps below | Calls `instance.predict(...)` with the inputs converted to `PredictionInput(data, file)` equivalents, in order. Requires a prior successful `load`; without one, reply `error` (the orchestrator always loads first — this is a sanity check, not a feature). **Without a `grant`** the whole `inputs` array goes to one `instance.predict` call, which is the pre-1b behaviour and the permanent compatibility path. **With a `grant`** the worker's packing harness splits `inputs` into several GPU batches within the granted budget and reports one measurement per batch. |
| `trim` | — | Release the caching allocator's **unused pool** (`torch.cuda.empty_cache()`), keeping weights, live tensors and the CUDA context. Valid in every state; a worker with no live CUDA does nothing and still replies `ok`. Reply carries a fresh memory sample (below). Never an error path: a trim that could not measure anything is still an `ok`. |
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

### Memory grants (optional `predict` request fields)

Added for batch calibration step 1b. The orchestrator is the budget arbiter
(it is the only component that sees every claimant on a GPU), so it decides
how much memory a window may use and the worker only *mechanises* that
decision. Both fields are optional; a worker that does not understand them
ignores them per the unknown-key rule and behaves exactly as before.

`grant` — the window's memory reservation, **dual-denominated**:

| key | meaning |
|---|---|
| `unit_budget` | how many cost-dimension units one GPU batch may contain. The packing currency; the worker prices its decoded inputs in the declared `unit` and packs greedily up to this number |
| `mb` | the MB the orchestrator has reserved out of the board's headroom for this window. The worker never spends against this directly — it is the reference for the **defensive clamp**: if live free memory has fallen below it, the batch is shrunk proportionally (shrink-only; the grant is never exceeded) |
| `unit` | `"item"` \| `"pixel"` \| `"token"` \| `"audio-second"` — the model's declared cost dimension |
| `aggregation` | `"count"` \| `"sum"` \| `"max-times-count"` — how per-item units combine into batch units |
| `user_cap_items` | optional per-request cap on **item count** per batch (the user-facing "max batch size"). Never converted to units; enforced as an additional bound at pack time |

`fit` — a snapshot of the orchestrator's fitted cost model, sent only when it
changed since the last frame this worker is known to have **received** (it is
pricing information, not per-window state). A window that failed or was aborted
may never have delivered its frame at all — and its per-request retries carry no
snapshot — so the orchestrator re-attaches the current snapshot to that worker's
next window. Re-sending an unchanged snapshot is harmless by construction: it is
advisory, and applying it twice is applying it once:

| key | meaning |
|---|---|
| `slope_mb_per_unit` | marginal driver MB per unit |
| `intercept_mb` | free intercept of the fit; diagnostic only |
| `residual_mb` | fit scatter (confidence) |
| `samples` | how many high-water samples the fit is built on |

**`fit` is advisory in v1.** The worker's defensive clamp compares live free
memory against `grant.mb` and scales the unit budget by that ratio; it does
**not** consume `slope_mb_per_unit` to convert MB into units. So a worker may
log the snapshot, expose it for diagnostics, or ignore it entirely — nothing on
the worker side changes behaviour based on it, and a worker that drops it is
still fully conformant. The field exists because the orchestrator already knows
the numbers and the channel is free; a slope-driven clamp is a later option,
not a current contract.

**Impls that sub-batch internally are not granted.** If the instance carries a
falsy `enable_batching` (or `enable_batch`) attribute, the worker ignores any
`grant` on the frame and takes the grantless compatibility path: the whole
window goes to one `instance.predict` call and no `units` are reported. Such an
impl decides its own GPU batch shape inside `predict`, so a granted batch's
size would not describe the allocator peaks that were measured — it is
`none`-class for calibration purposes until batching is re-enabled.

A worker with a `grant` **must** still return exactly one output per input,
in the original input order: bucketed packing reorders items internally
(`max-times-count` models pack similarly-sized neighbours together), and the
orchestrator splits outputs back per request by position.

A batch is never smaller than one item. A single item over budget goes
through alone; `run_with_oom_retry` inside the impl remains the backstop.

`ok` payloads:

- `handshake` → `protocol_version` (int): the version the worker speaks.
  v2 workers echo 2; the orchestrator kills workers that answer anything else.
- `configure`, `prewarm`, `unload`, `ping` → no extra fields.
- `trim` → optional `memory`: a memory sample taken **after** the
  `empty_cache()`, so its `reserved_mb` is the pool size the orchestrator
  should charge from now on. Absent when the worker could measure nothing
  (no torch, no live CUDA) — which is also when the trim itself was a no-op.
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
| `gpu_uuid` | the board the worker's CUDA device 0 actually resolved to, in nvidia-smi/NVML form (`"GPU-<uuid>"`). This — not the device-visibility variable the orchestrator spawned it with (`CUDA_VISIBLE_DEVICES`, or a bare device index in `HIP_VISIBLE_DEVICES` on ROCm) — is the authoritative GPU identity for the calibration ledger. Absent when the worker has no initialized CUDA device |
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
| `items` | number of inputs in the batch — a plain count |
| `units` | the batch's size in the model's declared cost dimension, as the packing harness priced it (`sum` of per-item units, `max × count`, or the item count). **Reported only when the batch ran to completion and the executed GPU batch matches the planned batch** — see below |
| `reserved_before_mb` / `peak_reserved_mb` | allocator pool size before the batch and its high-water mark during it |
| `allocated_before_mb` / `peak_allocated_mb` | live-tensor bytes before the batch and their high-water mark during it |
| `duration_ms` | wall time of `instance.predict(batch)` |
| `oom` | `true` when this batch raised an out-of-memory condition the harness observed, **or** when the impl's own halving loop absorbed one *anywhere* inside the `predict` call (an impl that calls `run_with_oom_retry` more than once per `predict` — a text tower and an image tower, say — has its halvings counted across all of those calls, not just the last). A negative sample for the orchestrator's deflation path; absent/false normally |
| `throughput_collapse` | `true` when this *pool-growing* batch was an upward-or-equal step in `units` against the previous pool-growing batch **and** its units/sec fell below the collapse ratio times that batch's. On Windows' WDDM the driver's sysmem fallback turns over-admission into a silent throughput collapse rather than an OOM, so this is the synthetic negative sample that stands in for the missing exception. A smaller (e.g. tail) batch or a non-growing one is not comparable and is never flagged; a flagged batch does not become the new comparator, so a persistent spill cannot normalise itself |
| `trimmed` | `true` on the **first** measurement of a window the worker's reactive shrink released the allocator pool before (see "Reactive shrink and trim"). Advisory: it explains why this batch grew the pool from (near) nothing and why its throughput is not comparable to the previous window's. Absent/false normally |

**`units` is reported only when the batch ran to completion and the executed
GPU batch matches the planned batch.** The number exists so the orchestrator can
regress allocator peaks against batch size, which requires the peaks and the size
to describe the same work. Several shipped impls sub-batch *inside* `predict` —
`run_with_oom_retry` with an `initial_chunk_size`, a per-image loop when the
impl's own batching is off, a hard-coded chunk of 1 — so the harness cannot
assume the batch it handed over is the batch the allocator saw. When the harness
can tell that the impl executed something smaller (it observes
`run_with_oom_retry`'s record of the largest chunk it actually ran), it **omits
`units` entirely**: the batch is unpriceable, and an unpriceable batch must never
reach the fit, because a `units` figure larger than the work behind the peaks
biases the fitted slope low and biases admission high. Two shapes of that record
are both unpriceable: a largest chunk *smaller* than the planned batch, and a
record that moved for this batch while reporting that **nothing** ran through the
helper (the impl consulted it, executed zero items there, and did the work by
another route — easyOCR's `readtext` fallback). A record that did not move at all
is different: the impl never consulted the helper for this batch, which is no
information, and the batch stays priceable. When the record shows the loop halved
at least once, the measurement additionally carries `oom: true` — the impl
absorbed an out-of-memory condition the orchestrator would otherwise never hear
about.

**A batch that failed is never priced.** Whatever the failure was — an
out-of-memory error, an assertion inside the impl, a processor that rejected an
input, an output-count mismatch — the allocator peaks describe however far the
call got before it gave up, which *understates* the cost of the batch that was
packed. So a measurement for a failed batch carries `items` and its peaks but no
`units`, on every failure path: an under-stated peak entering the fit as a clean
high-water sample would drag the fitted slope low, i.e. produce over-admission
out of a failure. The `oom` flag still rides the measurement and still drives
deflation, which is the part of a failed batch that *is* information.

Absent `units` is also the normal case for a request that carried no `grant`:
without a declared cost dimension the worker has nothing to price in.

`items` is deliberately *not* cost-dimension units: decoded pixels, tokens
and audio-seconds are known only where the inputs are decoded. `items` is
exact for the `item`/`count` class and nothing else; `units` is the priced
figure and is what the cost fit regresses against.

`duration_ms` covers the `instance.predict(batch)` call only — the harness's
own per-item unit pricing (image-header reads, byte counts) happens **before**
the timed section, deliberately, so the throughput-collapse comparator sees
GPU throughput rather than CPU decode noise. Decode *inside* the impl is
still inside the timing; nothing outside the impl can separate it.

The worker resets torch's peak counters before each measured batch, so peaks
are per-batch rather than cumulative. A worker that packs one request frame
into several GPU batches reports one entry per batch; a request with no
`grant` reports a single entry covering the whole `predict` call, which is why
the field is an array in both cases.

### Memory sensing on `error` frames

An `error` reply to `predict` may carry the same optional `memory` and
`measurements` fields. A window that failed part-way through still measured
whatever ran before the failure, and an out-of-memory batch is precisely the
sample the orchestrator most needs. The error semantics are unchanged (the
request failed, the worker stays alive); the fields are advisory telemetry.

A whole-batch out-of-memory condition the harness could not recover from is
additionally recognisable from the error *message*, which the orchestrator
classifies without parsing anything structured:

- `INFERENCE_OOM_BATCH_SIZE_1:` — a single input could not run (the
  pre-existing `InferenceOOMError` from `inferio.impl.utils`);
- `INFERENCE_OOM_WINDOW:` — a packed batch of more than one item raised an
  out-of-memory error that escaped the impl's own halving loop.

### Reactive shrink and trim

Releasing tensors does not give memory back: torch's caching allocator keeps
the pool. So when the board gets tight, *something* has to call
`empty_cache()`. There are two triggers, and they differ only in who notices
(docs/batch-calibration-design.md, "Reactive shrink" and "Trim for idle
residents"):

- **Reactive shrink** is the worker's own, and needs no protocol at all. Before
  a granted window's first batch the worker compares `grant.mb` against its
  live **releasable slack** — `memory_reserved() - memory_allocated()`, the
  blocks the caching allocator holds that no live tensor sits in, which is
  exactly and only what an `empty_cache()` can give back. When the grant has
  fallen materially below that slack for two consecutive windows it calls
  `empty_cache()` there — between batches, never inside one — and flags the
  window's first measurement `trimmed`. The comparison is deliberately *not*
  against `memory_reserved()`: the grant is an incremental activation
  reservation while the pool includes the weights, so that comparison is true
  nearly always and would tear down healthy pools every other window. Against
  slack the rule is also self-limiting — after a release there is no slack, so
  the next window cannot re-trigger. This only ever fires in a worker that is
  *receiving* windows.
- **Trim** is the orchestrator's, for a resident that is receiving none. An
  idle worker's retained pool squeezes its neighbours indefinitely and it will
  never notice, so the orchestrator sends it a `trim` request. It is a message
  on the existing request/response channel, in the same direction as `load`;
  the rejected proposal was a *worker*-initiated query channel, which this is
  not.

Trim is not unload. It releases only pool slack — weights, live tensors and
the CUDA context stay, so the model remains resident at a cost of milliseconds
plus re-`cudaMalloc` as the pool regrows. Unload frees `base` too, at full
reload cost.

Both events are **calibration opportunities**, not just hygiene: the batches
that regrow the pool afterwards are high-water batches, which are the only
ones the cost fit accepts. Both therefore also reset the worker's
throughput-collapse comparator — a post-`empty_cache()` batch is legitimately
slower than one on a warm pool, and comparing across the event would
manufacture a spurious `throughput_collapse`.

The orchestrator sends `trim` only to a replica it believes is **idle** — no
window in flight, no demand behind it, and none for the last few seconds. A
busy replica has its own shrink path and ignores or defers the request; one
window is in flight per worker either way, so a trim never races a batch.

## Lifecycle and timeouts (orchestrator side)

- Spawn → send `handshake` → response deadline (config, default 30 s).
  Timeout/exit/garbage → kill, surface stderr tail in the load error.
- `load` deadline is long (weights + dep imports; config, default 600 s).
- `predict` has no fixed deadline in v1 (arbitrary models); cancellation =
  kill the worker (it is the model — there is nothing softer to cancel).
- `trim` has a fixed 60 s deadline, and timing out is fatal. The operation is a
  `cudaFree` over every block in the allocator pool, which on a multi-gigabyte
  pool under a busy driver is not the milliseconds an idle `empty_cache()`
  costs — so the budget has to be well clear of a slow-but-healthy release
  while still bounding a best-effort hygiene message. It is deliberately not
  lowered by a smaller configured handshake deadline: that one is about spawn
  liveness, not about freeing a big pool.
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

- One device-visibility variable, when device pinning is active (absent =
  default). Which one, and in what vocabulary, is decided by the resolved
  accelerator (docs/rocm-batch-calibration-parity.md, D2):
  - `CUDA_VISIBLE_DEVICES` — CUDA hosts, and every host with no accelerator
    of its own. Normally a `GPU-…` board UUID; an unresolvable registry pin
    passes through as written.
  - `HIP_VISIBLE_DEVICES` — ROCm hosts, always a **device index** (HIP reads
    nothing else); a registry pin that cannot be resolved to one is *dropped*
    rather than passed through, so the variable is simply not written and the
    worker inherits the environment. `CUDA_VISIBLE_DEVICES` is deliberately
    *not* also set there: it is a HIP alias, and setting both is documented
    unintended-behaviour territory. `ROCR_VISIBLE_DEVICES` is never set —
    torch < 2.6 crashes at init when it is.

  Exactly one is written, and only when a pin resolved; a worker is never
  handed both.
- `INFERIO_WORKER=1` — marker for impl code that wants to know.
- `PYTHONIOENCODING=utf-8` — keeps worker stderr valid UTF-8 (defense in
  depth; the orchestrator's stderr forwarder tolerates arbitrary bytes from
  native code regardless).
- Inherited: `DATA_FOLDER`, proxy vars, PATH. Nothing else is promised.

The worker runs `python -m inferio_worker` with no arguments; everything it
needs arrives in the handshake.
