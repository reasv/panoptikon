# Inferio Worker Protocol v2

v2.1 (2026-08-01), additive, no version bump: `predict` output slots may
carry a typed per-item error instead of a payload (see "Per-item error
slots"). Absence of error slots is bit-for-bit the v2 behavior, so old
workers and the current orchestrator interoperate unchanged.

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

2026-08-01: MPS (Apple Silicon) reports memory like every other backend —
`free_source: "mps"`, `base_method: "mps"`, an authoritative `gpu_total_mb`,
and spawn-env lines for the allocator watermarks
(docs/unified-memory-admission.md, backend A). Additive values in existing
fields only, so the version stays 2.

2026-08-01 (backend C): a host with **no accelerator** reports memory too —
`free_source: "ram"`, `base_method: "rss"`, `gpu_total_mb` = physical RAM —
and the spawner tells such a worker which device to run on
(`INFERIO_DEVICE=cpu`, see "Environment"). Additive values in existing fields
plus one new env line, so the version stays 2.

2026-07-30 (step 2): the `trim` request type (see "Trim" below) and the
optional `trimmed` flag on a measurement. Also additive: an older worker
answers an unknown `type` with a per-request `error` and stays alive, which
is exactly how a trim to a worker that cannot do one should behave, so the
version still stays 2.

2026-09-04 (batch-calibration run2): six additive fields and one renamed
sentinel value, all of them consequences of what run1 measured
(`docs/batch-calibration-run1-report.md` §4). **Exactly these keys are new**,
and nothing else on the wire changed:

| key | where | run2 item |
|---|---|---|
| `canvas_pixels` | `predict` request, inside `grant` | R7 — the per-item pixel cap, from the registry key of the same name |
| `free_mb` | a measurement map | R5 — the pre-batch free reading the defensive clamp already takes |
| `free_source` | a measurement map | R5 — which driver produced that reading |
| `clamped` | a measurement map | R5 — `{from_units, to_units, free_mb}`, present only when the clamp shrank this batch |
| `oom_class` | a measurement map | R3 — `{source, exception, free_mb_at_failure, device}`, present only beside `oom: true` |
| `dtype_method` value `"unstated"` | `load` response | R11 — renamed from `"unknown"`, in `dtype` and `dtype_method` alike |

Additive in both directions: an older worker sends none of the response keys
and ignores `canvas_pixels`, an older orchestrator sends no `canvas_pixels`
and ignores the response keys, so the version stays 2. The one **non**-additive
line is the sentinel rename, which moves the calibration profile key for every
model that states no precision; see `dtype` below for why that is deliberate
and what it costs.

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
  many bytes of a single msgpack-encoded map. Max frame size 2 GiB
  (`0x8000_0000`); either side treats a larger declared *length* as a fatal
  protocol error (kill/exit). The orchestrator refuses to *send* an
  over-limit frame before any byte hits the stream — that refusal is a
  per-request failure (the worker stays alive), not a protocol error, and
  the dispatcher's byte-aware batch admission exists to make it unreachable
  in practice.
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
| `trim` | — | Release the caching allocator's **unused pool** (`torch.cuda.empty_cache()`, or `torch.mps.empty_cache()` on Apple Silicon), keeping weights, live tensors and the CUDA context. Valid in every state; a worker with no live CUDA does nothing and still replies `ok`. Reply carries a fresh memory sample (below). Never an error path: a trim that could not measure anything is still an `ok`. |
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
| `canvas_pixels` | **new (run2, R7)**: the model's *canvas* — the largest number of decoded pixels one input can actually cost it, whatever resolution the input was submitted at. Integer pixels; optional; meaningful only for a `pixel`-priced model. When present the worker prices every input at `min(raw_pixels, canvas_pixels)` before packing. The orchestrator reads it out of the registry (`metadata.cost.canvas_pixels`) and forwards it verbatim |

**The per-item pixel cap (`canvas_pixels`), and why it is a *pricing* field.**
Every `pixel`-class model shipped resizes or tiles its input onto a fixed
canvas before the first convolution — a tile grid, a `max_pixels` bound, a
detector's `canvas_size` — so its real cost stops rising at that canvas while
the worker's raw header-derived price keeps rising with whatever the user
submitted. Run1 measured both halves of what that costs (report §4, Q3/W1 and
F-B): a *fitted slope* that is a function of the corpus rather than of the
model (nemotron fitted 4.33x the probe's), 58 of 110 batches holding a single
item, and grants of 23-94 GB issued against a real footprint three orders of
magnitude smaller. Capping the price at the canvas makes the slope
corpus-independent and lets large images pack, and it errs in the one
direction that is safe to be wrong about: it *lowers* the priced units of a
big item, so the same grant buys a batch whose real footprint is the one the
fit was measured on rather than a batch of one.

The cap is denominated in **decoded pixels**, always, whatever the model's
declared `unit` — it describes the model's input geometry, not its pricing
scale. Two consequences the registry side enforces
(`panoptikon/src/inferio/cost.rs`): it is read only for a `pixel`-unit model
(capping a token count or an item count by an area is meaningless), and it is
never inherited from a group whose `unit` differs from the id's, the same
scale-bound rule `seed_units` already has — `[group.clip]` is `item`-priced
and its `qwen3-vl` / `nemotron` ids are not. A cap on a `count`-aggregated
model would in any case be inert by construction: every item already prices at
exactly one unit and `min(1, cap)` is 1.

Resolution order in the worker, and the documented fallback:

1. `grant.canvas_pixels`, when the orchestrator sent one. Authoritative: it is
   the figure the registry declares, so both sides speak one number.
2. otherwise, for `pixel` inputs only, the loaded impl's own known input
   resolution, if it exposes one: a positive integer `canvas_pixels`,
   `max_pixels` or `image_max_pixels` attribute on the instance, or on
   something reached from it through at most two processor-shaped attributes
   (`processor`, `image_processor`, `embedder`, `model`) — one level reaches
   `instance.embedder.max_pixels`, two reach `instance.model.processor.*`,
   which are the two shapes the shipped impls actually have. Read passively —
   the worker never constructs anything and never imports anything to ask —
   and floored at 512x512 pixels, below which a reading is treated as a
   misidentified attribute rather than a canvas. Too *small* a cap is the one
   direction that hurts (it under-prices an item, which over-admits), so the
   floor is the guard, and the resolved source is logged once per process.
   This tier is what covers a model whose canvas the registry cannot state
   statically because it lives in the downloaded processor's config
   (`doctr/dots_ocr`).
3. otherwise uncapped, exactly as before this field existed.

The registry declaration the orchestrator reads it from:

```toml
[group.clip.inference_ids.nemotron-embed-vl-1b-v2]
metadata.cost.unit          = "pixel"
metadata.cost.aggregation   = "sum"
metadata.cost.seed_units    = 2000000
metadata.cost.canvas_pixels = 1835008   # (6 tiles + thumbnail) x 512^2
```

Absent = uncapped, which is what every model did before run2.

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
  either msgpack `bin` (bytes output, e.g. serialized numpy), an **error
  slot** (below), or any other msgpack value (JSON-like output). This mirrors
  what impl `predict()` returns today: `bytes` stay bytes, everything else is
  data. Plus the optional memory-sensing fields below.

### Per-item error slots (predict)

An output slot may report that *its own input* failed instead of carrying a
payload. This exists so one undecodable file can no longer take its healthy
batch-mates down with it, and so the component that actually decodes the media
(PIL in the worker, with `LOAD_TRUNCATED_IMAGES` on) is the one that calls it
bad — the gateway never pre-judges media on a worker's behalf
(`docs/failed-media-retry-design.md`).

Wire shape — a map with the reserved key `__error__`:

```
{"__error__": {"class": "input" | "transient", "message": <str>}}
```

- `class` `"input"`: the worker's own decoder rejected **this input's
  payload**. This is the only class a consumer may treat as a verdict on the
  media. Nothing else — not OOM, not a model error, not a missing dependency
  — may be reported this way; those keep failing the whole `predict`, which
  is what the orchestrator retries.
- `class` `"transient"`: this slot failed for a reason that says nothing about
  the payload. Reserved for future worker use; consumers count and retry it
  and never persist it. Because it is a request to retry, a consumer may not
  keep only the batch-mates that succeeded and call the item done: one
  `transient` slot fails the whole item transiently, even when the rest of
  the response is fine. (An `input` slot among survivors is the opposite: it
  is a settled verdict on that unit, so the item proceeds without it.)
- `message`: human-readable detail, carried into logs and the failure ledger.

Rules:

- **The count still has to match.** An error slot occupies its input's
  position; `outputs.len() == inputs.len()` remains a hard invariant, so slot
  alignment (and the dispatcher's batch splitting) is unchanged.
- **Absence is today's protocol.** A worker that never emits error slots
  behaves exactly as before, and a gateway that receives none takes exactly
  the old code paths. Workers and orchestrator ship from one repo, so a
  worker emitting slots always meets an orchestrator that understands them;
  the protocol version is not bumped for this additive shape.
- **Malformed is fatal.** A map carrying `__error__` whose body is not an
  object, whose `class` is missing or unknown, or whose `message` is missing
  or not a string, is a protocol violation and is treated like a count
  mismatch (the worker is killed). Guessing would let a broken worker
  fabricate an "undecodable media" verdict, which the gateway persists.
- Ordinary outputs are never inspected for this key beyond the top level: a
  payload map without `__error__` is a payload, and nested occurrences are
  ignored.

The gateway's HTTP predict surface mirrors the same shape additively: a batch
containing an error slot is always encoded as the JSON `{"outputs": [...]}`
envelope (the raw/multipart binary encodings have nowhere to put a typed
failure), where surviving binary outputs keep their
`{"__type__": "base64", "content": ...}` wrapper and failed slots are the
`{"__error__": {...}}` object above. Batches without error slots keep the
byte-identical legacy encoding.

### Desired in-flight items (HTTP predict response header)

`POST /api/inference/predict/{group}/{id}` answers with an additive, optional
response header:

```
x-panoptikon-desired-in-flight-items: <positive integer>
```

It is the number of **items** the orchestrator would like the caller to keep
inside its in-flight predict requests for that model — items and PDF pages,
never pixels, tokens or seconds. It exists because the two sides own
different halves of batch sizing (`docs/batch-calibration-design.md`, "Batch
size UX", split #2): the orchestrator owns the VRAM picture (the ramp, the
anchor, the knee, the board's headroom) and the caller sizes its requests "by
keeping the server fed" without ever learning about VRAM. This header is the
whole of what crosses that boundary.

It is a header, not a body field, because a predict answers in three
encodings — `application/octet-stream`, `multipart/mixed` and the JSON
`{"outputs": [...]}` envelope — and only one of them has anywhere to put a
scalar. Every existing client ignores it, and the header is a *response*
header, so the gateway's inbound `x-panoptikon-*` strip does not apply.

How the orchestrator derives it, per model (`inferio/dispatch.rs`,
`desired_in_flight_items`):

- **Priced models** (a known board and a cost dimension that scales): the
  dispatcher's current window target in units — the ledger's admitted unit
  budget times `WINDOW_DEPTH_MULTIPLIER` — converted into items through the
  most recently formed window's items-per-unit ratio, or, before any window
  has been formed, through the unit class's seed estimate (1 for
  `item`-priced and for every `count`-aggregated model, ~2 MP for `pixel`,
  ~512 tokens for `token`, 30 s for `audio-second`). Multiplied by a slack of
  2 so that consecutive windows can merge, then bounded by the same
  payload-byte wall the dispatcher applies to a window (`MAX_WINDOW_BYTES`)
  converted through that window's bytes-per-item — past the byte wall a window
  cannot merge another request anyway, so more work in flight buys nothing.
- **A squeezed window publishes the budget it was granted**, not the target it
  asked for. When the board cannot afford the admitted unit budget the ledger
  issues a smaller one and flags the grant *squeezed*; the figure is then
  derived from that granted budget (times the same window depth and slack)
  rather than from the anchor-derived target. Publishing the target under
  pressure would keep the caller queueing work for memory the board does not
  have, and the resulting window then runs for as long as it takes to chew
  through it at the squeezed batch size, with no re-pricing in between — the
  more squeezed the grant, the longer the server runs on a stale picture. The
  figure still never falls below several of the batches the worker was
  actually given, and an unsqueezed grant restores the target on the very next
  window (the figure is derived from the anchor target and *this* window's own
  grant, never from the already-clamped bound the window was formed under, so
  one unsqueezed grant is enough and an alternating squeeze still publishes
  the target on its unsqueezed windows). The same clamp bounds the *window*
  the dispatcher forms next, since the header can only shorten a caller's
  pipelining and cannot shorten a window that is already formed (nor oblige a
  caller to honour it at all) — and that window bound has no floor, whereas
  the published figure meets core's own floor of 64 items
  (`MIN_IN_FLIGHT_UNITS`). Under a hard squeeze — below ~11 granted units at
  one unit per item, which is where `budget x 3 x 2` falls under 64 — the
  header stops being what shortens the blind window and the window clamp is
  the whole of it.
- **Unpriced paths** — `none`-class (grantless) models, a host with no GPU
  inventory, a board outside the enumeration — have no unit target and no
  worker-side packer, so the frame the worker receives *is* the GPU batch and
  its size is the fixed `default_batch_size`/`default_max_batch`. The figure
  is that fixed size times the same slack of 2. The user's `max_batch` cap is
  deliberately not folded in: a cap bounds the GPU batches the server forms,
  never how much work the caller keeps in flight.

The header is **absent** when the orchestrator has no opinion: a Python-era
inference server, a model that has not dispatched a window yet, or a model
unloaded between the predict and the response encoding. Absent means **no
change from the last figure the caller was given** — never an error, and
never a figure of zero. A caller's *initial* value is its own floor, so a
server that never sends the header leaves the caller at that floor for the
whole run, which is the pre-feature behaviour; a server that sends the header
and then misses one response does not lose the figure it already published.
A caller must never require the header.

What the caller does with it is the caller's business. The gateway's own
extraction jobs (`jobs/extraction.rs`) resize a per-job unit semaphore toward
the figure on every response, clamped between a floor of one request's worth
(64 units, which is also the value the semaphore starts at) and a ceiling
derived from the job's intermediate byte budget, loader slots and descriptor
budget (4096 units at the shipped defaults). Growth adds permits; a shrink is
applied only to permits that are free, and the remainder is withheld as
outstanding permits come back, so a resize never interrupts work already in
flight.

**A caller must bound the figure by its own file-descriptor budget.** The
figure is sized by the *server's* memory picture and the server cannot see the
client's `RLIMIT_NOFILE`, so honouring it literally is how a caller runs out
of sockets: each in-flight predict costs one client socket, and when the
inference server is the same process (the gateway's `inference_local`) it
costs a second for the accepted end, so N items in flight cost up to 2N
descriptors in one descriptor table on top of databases, listeners and worker
pipes. The gateway therefore raises its own soft limit to the hard limit at
startup and caps the ceiling above at
`(soft_nofile - 256) / 2` (`jobs/extraction.rs`, `FD_RESERVE` and
`FDS_PER_IN_FLIGHT_ITEM`). Ignoring a published figure — downward, never
upward — is always allowed.

### Memory sensing (optional response fields)

Added for batch calibration (`docs/batch-calibration-design.md`): the worker
is the only side that has torch, the allocator statistics and the decoded
input, so it reports what it measured and the orchestrator does all the
sizing. Every field here is **optional and additive** — a worker with no
torch, no GPU runtime, or no NVML/amdgpu-sysfs source omits what it cannot measure, and the
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
| `free_source` | which driver told us: `"nvml"`, `"amdgpu-sysfs"` (amdgpu's `mem_info_vram_total - mem_info_vram_used` for the worker's own board), `"mps"` (Metal's `recommended_max_memory` bounded by the OS's available-RAM figure — see below), `"ram"` (the machine's own RAM statistics on a host with no accelerator — see below) or `"torch"` (`mem_get_info`). Absent/nil when none could answer |
| `reserved_mb` | torch caching-allocator pool size (`memory_reserved`); on a `"ram"` host, this process's OS high-water resident set |
| `allocated_mb` | live tensor bytes (`memory_allocated`); on a `"ram"` host, the live RSS |

`free_mb`/`total_mb` always come from **one** source, named by `free_source`.
The two do not agree — NVML sees the whole board, `mem_get_info` the calling
context's view (measured 3.4 GB apart on the dev box) — so a consumer that
differences two samples, or subtracts our own footprint from `free_mb` to
estimate what other processes hold, must only compare readings whose
`free_source` matches. The worker tries `nvml`, then `amdgpu-sysfs`, then
`torch`, on every host — each tier's own availability is already the platform
test (`nvmlInit` fails permanently on a ROCm host; `mem_info_vram_*` exists
under no other driver's PCI directory), so the effective order is
`nvml → torch` on CUDA and `amdgpu-sysfs → torch` on ROCm. The two
driver-level sources see the whole board and answer *before* this process has
a context, which `mem_get_info` cannot do without creating one; `torch` is
last-resort on both backends, and on HIP doubly so — its "free" was
historically process-local (ROCm/hip#348). Consumers treat `"nvml"` and
`"amdgpu-sysfs"` as authoritative whole-board readings and `"torch"` as not.
`"amdgpu-sysfs"` names the *driver*, not the filesystem, so a future generic
sysfs-derived reporter cannot inherit that authority by string collision — and
it is the same pair of files the orchestrator's own refresh reads, so both
sides of the ledger speak one vocabulary by construction.

**`"amdgpu-sysfs"` is GTT-inclusive on a verified unified board**
(`PANOPTIKON_UNIFIED_GPU` naming the address the worker itself resolved)
(docs/unified-memory-admission.md, backend B). On an AMD APU the label and the
files are the same, but the arithmetic covers the whole board an APU actually
has: `total = mem_info_vram_total + mem_info_gtt_total` (the BIOS UMA
carve-out plus the GTT window its allocations spill into as soon as the
carve-out fills), and `free = (vram_total - vram_used) + min(gtt_total -
gtt_used, ram_available)`, with `ram_available` from
`psutil.virtual_memory().available`. The clamp is the load-bearing part:
unclaimed GTT is address space, and the pages behind it come out of the same
RAM every other process is using, so without it a machine under real memory
pressure would read as idle. Every term is required — a board whose GTT
counters or whose RAM figure cannot be read reports *no* sample rather than a
VRAM-only one under a label that now means something else. The orchestrator's
refresh applies the identical formula to the identical files for the boards
its own probe flagged unified, so the one-vocabulary rule holds here too; the
env var exists because the worker has no inventory and cannot tell the two
kinds of board apart (see "Environment" below). With the variable absent every
reading is byte-identical to a discrete board's, which is what keeps every
dGPU worker's numbers where they were.

**`"mps"` is the unified-memory reading** (docs/unified-memory-admission.md,
backend A). `total_mb` is Metal's `recommendedMaxWorkingSetSize`
(`torch.mps.recommended_max_memory()`) — the policy budget for accelerator use
of the machine's RAM, which is what allocations are actually judged against and
is *not* a constant: raising the GPU wired limit moves it. `free_mb` is
`max(0, min(total, ram_available))`, with `ram_available` from
`psutil.virtual_memory().available`. The RAM term is the load-bearing part: the
memory is the whole machine's, so external pressure has to be read from the OS
— there is no accelerator-level free counter on that board at all — and a
browser eating 40 GB then shows up exactly the way a game eating VRAM does on a
dGPU. It is treated as authoritative (whole-machine by construction), and the
orchestrator's own refresh reads the same statistics under the same label:
psutil's macOS `available` is free + inactive pages, and the orchestrator's
`host_statistics64` read sums exactly those two terms and no others, so
neither side is systematically the looser of the two.

**`"ram"` is the CPU-priced host's reading** (docs/unified-memory-admission.md,
backend C), and it is the degenerate case of the unified model: there is no
accelerator pool to intersect with, so `total_mb` is physical RAM and
`free_mb` is `psutil.virtual_memory().available` bounded by it. It is
authoritative — whole-machine by construction, and the only reading such a
host has — and the orchestrator's own refresh reads the same sources under the
same label (`MemTotal`/`MemAvailable` on Linux, `GlobalMemoryStatusEx` on
Windows, free+inactive pages on macOS).

**The tier is gated on the spawner's `INFERIO_DEVICE=cpu`, not on the absence
of an accelerator**, and is checked *before* every other tier rather than
after. Both halves matter. A worker with no torch, no NVML and no HIP pin is
also what a remote-API impl or a `none`-class model looks like on a CUDA host;
reporting host RAM there — under a label consumers treat as authoritative,
against a board whose total is a card's VRAM — is precisely the
different-currency error the ledger's own total check exists to catch. How a
host was priced is a fact only the orchestrator has, so it states it. And on a
host it *has* stated, no accelerator tier may answer either: such a machine can
have an NVIDIA card in it whose NVML would happily describe a board nothing is
running on.

`load` `ok` may additionally carry:

| field | meaning |
|---|---|
| `base_mb` | the worker's whole-**process** device footprint after load (CUDA context + workspaces + weights), not just its allocator footprint; on a `"ram"` host, the growth of the process's resident set across the load window. Absent — never zero — when the process demonstrably put nothing on the device it is priced against (no torch, a remote API, or a torch-importing engine like CTranslate2 whose VRAM the allocator never sees) |
| `base_method` | how `base_mb` was obtained: `"nvml"` (own-PID `usedGpuMemory`), `"fdinfo"` (this process's own VRAM on its own board per DRM fdinfo — NVML's ROCm twin, same rank, HIP-only), `"mps"` (`torch.mps.driver_allocated_memory()` at load end — per-process *by construction*, since each process owns its Metal heap, so it is the same rank as the other two and needs neither a PID lookup nor a plausibility floor), `"rss"` (the growth of this process's resident set across the load window, on a `"ram"` host — see below), `"free_delta"` (driver free-memory delta across the load), `"alloc_delta_measured"` (**new in run2, R8**: allocator peak delta plus the accelerator context this process *measured* itself, as the board free-memory delta across the first CUDA initialisation, taken before the impl allocated anything) or `"alloc_delta"` (allocator peak delta plus the fixed context allowance — the same formula with an assumed context instead of a measured one, and the last resort when no free reading was available to measure with). Always names the term that actually produced the reported number, and the two `alloc_delta*` spellings are two different formulas precisely so a stored profile cannot claim a measured context it never had |
| `reserved_at_load_mb` | allocator pool size right after load; the orchestrator prices later pool growth against this |
| `dtype` | the load precision in use, one of `"fp16"`, `"bf16"`, `"fp32"`, or `"unstated"` (part of the calibration profile key). **`"unstated"` is a value, not a failure**: the key needs every component or the entry can never be read back, and only four shipped impls negotiate a precision through `select_dtype`, so an omission here silently costs every other model its whole stored profile. It is stable for a given impl, so an entry written under it is found again by the next run; the day that impl does negotiate one, the key moves and the old row is ignored exactly as a dtype *change* is. Absent only when the report carries no `base_mb` either — nothing to key, nothing to persist, and a worker that measured nothing answers exactly as it did before any of this existed. **Renamed in run2 (R11): the sentinel used to be spelled `"unknown"`.** It says the impl stated no precision, which is not the same fact as the worker having failed to look, and a key component that reads as a failure invites a consumer to treat it as one. The rename moves the profile key, so every profile stored under the old spelling stops matching and is ignored exactly as a stale epoch is — deliberate, and cheap, because the sentinel was introduced during run1 and nothing has been released under it |
| `dtype_method` | how `dtype` was arrived at: `"selected"` (the impl negotiated it — `inferio.impl.utils.select_dtype`, or an instance `resolved_dtype`), `"attribute"` (a real `torch.dtype` held on the instance), `"inferred"` (read off the loaded weights: the first floating-point parameter, else buffer, of the first `torch.nn.Module` found on the instance or one level inside it) or `"unstated"` (nothing answered — a CTranslate2/ONNX engine, a remote API). Additive and **diagnostic only**: nothing keys on it, and the profile is keyed on `dtype` whichever method produced it. Reported whenever `dtype` is. **Renamed in run2 (R11) with the `dtype` sentinel above, from `"unknown"`**: one vocabulary, one rename — a `dtype` of `"unstated"` and a `dtype_method` of `"unstated"` are the same fact stated twice, and leaving the method spelled the old way would have made them look like different ones |
| `gpu_uuid` | the board the worker's CUDA device 0 actually resolved to, in nvidia-smi/NVML form (`"GPU-<uuid>"`). This — not the device-visibility variable the orchestrator spawned it with (`CUDA_VISIBLE_DEVICES`, or a bare device index in `HIP_VISIBLE_DEVICES` on ROCm) — is the authoritative GPU identity for the calibration ledger. Absent when the worker has no initialized CUDA device, **and always absent on a ROCm (HIP) build** — see below |
| `gpu_name` | that board's marketing name as torch reports it (e.g. `"NVIDIA GeForce RTX 5090"`), informational. The calibration profile key uses the orchestrator's own inventory name for the board, not this. On MPS torch has no board struct to ask, so the worker derives `"Apple M3 Max (128 GB)"` from the same two sysctls (`machdep.cpu.brand_string`, `hw.memsize`) and the same rounding the orchestrator's probe uses — deliberately identical, so the one field that could silently drift from the profile key does not. On a `"ram"` host it is `"CPU (64 GB)"`, derived the same way from physical RAM and the same round-up-to-4-GiB rule |
| `gpu_bdf` | the board's PCI address as the worker read it from `get_device_properties(0)`'s `pci_domain_id`/`pci_bus_id`/`pci_device_id`, rendered `"dddd:bb:dd.0"` in lower-case hex. The function digit is always `.0`: the GPU function of an amdgpu device is 0 (the HDMI/DP audio controller is `.1` of the *same device*), which is how the orchestrator's own probe renders it too, so the two sides join. Reported on CUDA hosts as well — additive, and harmless where the UUID already identifies the board. Absent on a torch build that exposes no PCI fields, unless the fdinfo fallback below answered — which today means absent on the shipped CUDA build, whose venv pins torch 2.7.1 (`_CudaDeviceProperties` grew the PCI fields in 2.8, and the fdinfo fallback is HIP-only): this field goes live on CUDA when that pin moves to >= 2.8, and until then the identity chain it feeds is load-bearing on ROCm alone (the `rocm` extra pins torch 2.11) |
| `gpu_total_mb` | that board's total VRAM per torch (`get_device_properties(0).total_memory`), in MiB. Deliberately a *second* source for a number the orchestrator can also read from the driver: it is what a non-UUID board match is cross-checked against. **On MPS it is `recommended_max_memory()` and it is not a cross-check but the authoritative figure**: the orchestrator seeds that board's total at ≈75 % of RAM (Metal's default) and adopts the reported number on the first load report, sanity-bounded by physical RAM alone — a raised GPU wired limit legitimately puts the real figure 20 % away from the seed (docs/unified-memory-admission.md, DP-4). **On a `"ram"` host it is physical RAM**, and it is a cross-check again — the strictest in the design, since both sides read the same kernel fact and are expected to agree exactly. It is also what makes such a worker identifiable at all: registration's single-board fallback needs a report that claims a board, and RAM is the only thing this one has to claim. It is emphatically not adopted — the orchestrator read that number itself at probe time |
| `torch_version` | `torch.__version__` (e.g. `"2.7.1+cu128"`), part of the calibration profile key. Only the worker knows which torch its venv holds. Absent when the impl never imported torch |
| `memory` | a memory sample taken right after load |

**Board identity across backends.** On CUDA the identity is `gpu_uuid`, which
is byte-identical to what the orchestrator's inventory holds, so registration
is an exact match and nothing else is consulted. On ROCm there is no such
string: torch >= 2.5 renders a UUID from the ASIC serial, but it is a *third*
vocabulary — matching neither KFD's `GPU-<16 hex>` nor amd-smi's 8-4-4-4-12
form — and on consumer boards without a fused serial it is identical for every
card of a model. The worker therefore reports **no `gpu_uuid` at all** when
`torch.version.hip` is set, and the orchestrator keys those replicas on
`gpu_bdf`, cross-checking `gpu_total_mb` against the board's own total before
admitting them (±5% or ±512 MB). A match it cannot cross-check is refused, and
a refused replica simply dispatches unpriced — the pre-calibration behaviour
(`docs/rocm-batch-calibration-parity.md`, D3).

On a **unified** ROCm board (an APU) that cross-check accepts **either** of
two figures, each within the same tolerance: the board's admission total
(carve-out + GTT) or the BIOS carve-out alone. Which of them HIP reports as
`total_memory` there is genuinely unknown until the BC-250 field pass, and
refusing on the unknown would leave every APU host unpriced — the state
backend B exists to end. The either-of rule widens the check by exactly one
candidate; a figure that is neither is still refused
(docs/unified-memory-admission.md, backend B).

`gpu_bdf` has one fallback source, used only on a ROCm build whose torch is
too old to expose the PCI fields: the DRM client holding the most VRAM in
`/proc/self/fdinfo` (`drm-pdev` + `drm-client-id`, with `drm-resident-vram` or
its deprecated amdgpu alias `drm-memory-vram`, deduplicated by client id). It
answers nothing unless one board strictly dominates, because a HIP-pinned
process still holds render nodes for every ROCr-visible board — the pinned one
is merely the one it allocated on.

The same fdinfo parse, filtered by that identity address instead of ranked by
it, is what `base_method: "fdinfo"` reads: an absolute whole-process footprint
on the worker's own board, which is what `base_mb` is defined as, obtained
without root, without amdsmi and without NVML's PID-namespace caveat (the
worker reads *itself*). It is HIP-only and it is floored: fdinfo's memory stats
for compute allocations are VM-walk-based and need a recent kernel, so a
reading materially below the worker's own allocator pool is rejected as an
under-report and the coarser tiers answer instead — an under-measured base is
headroom the ledger would hand out twice. The comparand is the **absolute**
post-load pool (`reserved_at_load_mb`), not the load window's growth: fdinfo
reports absolute whole-process VRAM, so the two only coincide on a process's
*first* load, and a windowed comparand would wave an under-report through on
every reload into an already-loaded worker. The windowed delta is the fallback
for the one case that leaves the absolute figure unknown — the allocator could
not be read after the load at all.

On a verified unified board that footprint is VRAM **+ GTT**, for the same
reason the sample above is, and the tier's *upper* sanity bound (a reading at
or above the board's own capacity is a parse or accounting artefact, not a
footprint) is measured against **carve-out + GTT** there rather than against
what HIP reports as `total_memory` — which may be the BIOS carve-out alone,
512 MB being a common default, a figure any GTT-inclusive footprint worth
measuring exceeds. The bound is kept, with the comparand the sysfs tier
already reads; the under-report floor is unchanged and is still the one that
matters most, because too small is the direction the ledger cannot absorb.

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
| `oom` | `true` when this batch raised an out-of-memory condition the harness **classified** as one (see `oom_class`), **or** when the impl's own halving loop absorbed one *anywhere* inside the `predict` call (an impl that calls `run_with_oom_retry` more than once per `predict` — a text tower and an image tower, say — has its halvings counted across all of those calls, not just the last). A negative sample for the orchestrator's deflation path; absent/false normally. **Changed in run2 (R3):** a failure the classifier does not recognise now leaves this absent, where before any error text containing the words "out of memory" set it — run1 measured 15 spurious negatives on a board with 96 GB free from one impl's wording (finding Q1/B11) |
| `throughput_collapse` | `true` when this *pool-growing* batch was an upward-or-equal step in `units` against the previous pool-growing batch **and** its units/sec fell below the collapse ratio times that batch's. On Windows' WDDM the driver's sysmem fallback turns over-admission into a silent throughput collapse rather than an OOM, so this is the synthetic negative sample that stands in for the missing exception. A smaller (e.g. tail) batch or a non-growing one is not comparable and is never flagged; a flagged batch does not become the new comparator, so a persistent spill cannot normalise itself |
| `trimmed` | `true` on the **first** measurement of a window the worker's reactive shrink released the allocator pool before (see "Reactive shrink and trim"). Advisory: it explains why this batch grew the pool from (near) nothing and why its throughput is not comparable to the previous window's. Absent/false normally |
| `oom_class` | **new (run2, R3)**: present exactly when `oom` is `true`, as `{source, exception, free_mb_at_failure, device}` — *why* the harness called this an out-of-memory condition, so the orchestrator can trust a structural signal and corroborate a textual one instead of guessing from a message it never sees. Absent when `oom` is absent, and **absent means the worker saw no out-of-memory condition**, including on a batch that failed for some other reason: the orchestrator must not deflate on such a failure |
| `free_mb` | **new (run2, R5)**: driver-reported free memory on the worker's board, read immediately **before** this batch ran — the very sample the defensive clamp compares against `grant.mb`, reported rather than discarded. Absent when nothing could be read, and absent on the grantless compatibility path, which takes no pre-batch reading |
| `free_source` | **new (run2, R5)**: which driver produced `free_mb`, from the same vocabulary a memory sample's `free_source` uses (`"nvml"`, `"amdgpu-sysfs"`, `"mps"`, `"ram"`, `"torch"`). Present exactly when `free_mb` is |
| `clamped` | **new (run2, R5)**: present only when the defensive clamp actually **reduced** this batch's budget, as `{from_units, to_units, free_mb}` — the granted per-batch unit budget, what it was shrunk to, and the free reading that forced it. Absent on every batch that ran at its granted budget |

`oom_class` has four keys:

| key | meaning |
|---|---|
| `source` | `"typed_exception"` — the failure *was* an allocator exception the worker could name by type (`torch.OutOfMemoryError`, which is the same class on CUDA and on HIP builds, or the interpreter's own `MemoryError` for host RAM). Structural: no text was consulted, and the orchestrator can act on it alone. `"marker"` — an `INFERENCE_OOM_*` marker raised by the impl helper (`inferio.impl.utils.run_with_oom_retry` gives up at a single item), or that helper's halving counter moving inside the call. Also structural: the marker is our own code stating a classification it made from a typed exception one frame lower. `"message_pattern"` — none of the above matched and the text carried one of a closed list of **driver-shaped** strings (`cuda out of memory`, `hip out of memory`, `hip error: out of memory`, `cuda error: out of memory`, `mps backend out of memory`, `defaultcpuallocator` + `allocate memory`, `enforce fail at alloc_cpu.cpp`, `cublas_status_alloc_failed`, `cudnn_status_alloc_failed`, `cudaerrormemoryallocation`, all matched case-insensitively). A bare `out of memory` substring is deliberately **not** on that list: it is the one match run1 found firing on a healthy model. MPS is the reason the list exists at all — an MPS allocation failure is a plain `RuntimeError` whose message is its only signal, and there is no other form of it |
| `exception` | the failing exception's type name, qualified when the type is not a builtin (`"torch.OutOfMemoryError"`, `"RuntimeError"`, `"MemoryError"`). The literal string `"run_with_oom_retry"` when the classification came from the halving counter rather than from an exception — a batch that *succeeded* after the impl absorbed an out-of-memory condition internally has no exception to name |
| `free_mb_at_failure` | free memory on the worker's board, read at the moment of the failure. `null` when nothing could be read. This is the corroboration a `message_pattern` classification needs before the orchestrator deflates on it: an out-of-memory claim made while the board has tens of GB free is a wording, not a condition |
| `device` | which device the two memory figures describe, as `"<backend>"` or `"<backend>:<board uuid>"` (`"cuda:GPU-1234…"`, `"rocm"`, `"mps"`, `"cpu"`, `"unknown"`). It exists so a reading can never be attributed to the wrong board on a multi-GPU host |

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

**On MPS the peaks are an approximation, and a documented one.** torch.mps
exposes no peak counters and no reset, so `peak_reserved_mb` /
`peak_allocated_mb` are `driver_allocated_memory()` / `current_allocated_memory()`
read *after* the batch. The caching pool is *usually* monotone between
`empty_cache()` calls — the same property the CUDA pool has — so the
post-batch driver allocation is normally the batch's high-water reserved size;
the orchestrator's own reactive shrink runs strictly between windows and so
never releases the pool mid-batch. **The exception is the ceiling itself**:
the MPS allocator garbage-collects cached buffers when an allocation would
cross the low watermark, so on a batch that ran close to the budget the
post-batch figure can sit *below* the true peak. The bias is therefore toward
under-stating cost exactly where cost matters most, which is why the spawn env
pins both watermarks to 1.0 (below) and why the collapse detector and the
death-as-negative signal (DP-2) carry the near-ceiling regime rather than the
peak arithmetic. Verifying the size of the effect is an M3 Max field-pass item.
The allocated figure is the weaker of the two: it is live tensors at
the end of the call rather than at their peak, so it understates a transient.
Nothing in admission regresses against it — the fit uses reserved growth — and
it stays in the sample as the diagnostic it is elsewhere.

**On a `"ram"` host the pool *is* the OS high-water mark**, and that mapping —
rather than "RSS is the pool and the high-water is the peak" — is the decision
(docs/unified-memory-admission.md, backend C). The high-water is a genuine
peak, recorded by the kernel as it happens rather than sampled afterwards, so
unlike MPS nothing is lost between two readings. What it is not is
*resettable*: no platform offers a reset for it, so it is monotone for the
process's whole life — which is exactly the shape of the CUDA caching
allocator's pool, and reporting it as `reserved_mb` / `peak_reserved_mb` is
what keeps `peak > before` meaning "this batch grew the envelope" here as
everywhere else. The knee's warm/high-water split, the cost fit's
`peak_reserved − reserved_at_load` and the WDDM throughput comparator all keep
their meanings unchanged. `allocated_mb` / `peak_allocated_mb` are the live
RSS, which understates a transient exactly as MPS's live figure does; nothing
in admission regresses against it.

What the monotone pool costs is worth stating exactly, because it is not a
uniform over-statement. `reserved_at_load_mb` is the high-water at load end
and therefore includes the load's own transient, so it sits above the settled
figure. A batch that stays under that mark sets no new high-water and reads as
*warm*: no fit sample, no ratchet anchor, and a model whose working set never
exceeds its load transient simply never confirms its cost model. A batch that
does exceed it prices at `peak − reserved_at_load`, i.e. with a constant
**negative** intercept of roughly the load overshoot — under-pricing, bounded
by that overshoot and self-correcting as the geometric ramp raises the mark,
with the residue landing in the external term via the RAM free reading. It is
the same effect the CUDA fit already carries occasionally (a load whose pool
overshot its weights; see the note beside `FitSample` in
`panoptikon/src/inferio/ledger.rs`), systematic rather than incidental here.

The sources are per-platform and **their units differ**: `VmHWM` in
`/proc/self/status` on Linux (kibibytes, despite the `kB` spelling),
`psutil`'s `memory_info().peak_wset` on Windows (bytes), and
`resource.getrusage(RUSAGE_SELF).ru_maxrss` elsewhere — **bytes on macOS,
kibibytes on every other Unix**, a factor of 1024 either way. A high-water
below the live RSS is never reported: the two come from different interfaces,
and a "peak" under the current reading is a reading of nothing.

**Trim releases nothing on a `"ram"` host.** It still answers `ok` with a
fresh `"ram"` sample, like any successful trim, and the sample is unchanged
because nothing was freed: there is no allocator pool to hand back, and Python
frees into the glibc/CRT arenas, which keep their pages. This is decided
rather than missing (docs/unified-memory-admission.md, "Trim") — `malloc_trim`
exists only on glibc, returns only the top of the main arena, and would need a
ctypes platform branch for a release the footprint accounting already covers.

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
- **Every worker is forked from one permanent thread.** The spawn arms
  `PR_SET_PDEATHSIG` (SIGKILL) so that a gateway death which runs no
  destructors still reaps the worker — but on Linux the kernel delivers that
  signal when the forking **thread** exits, not when the process does. Tokio
  retires threads routinely (its multi-thread workers are blocking-pool
  threads, and a `block_in_place` demotes one into that pool, which reaps it
  after a 10 s idle keep-alive), so a worker forked by "whichever thread got
  here" is killed by the kernel seconds later, mid-inference, with no
  traceback (finding F11: 8/8 deaths, 1–3 ms after the forking thread's
  `exit`). `process_tree::spawn_supervised_tokio` therefore funnels every
  armed spawn through a single dedicated thread that never exits; the child
  is created inside the caller runtime's `Handle::enter()`, so tokio's
  SIGCHLD/reaping machinery still belongs to the runtime that will `wait()`
  on it. The user-visible death contract is unchanged: worker processes
  still die with the gateway.
- `load` deadline is long (weights + dep imports; config, default 600 s).
- **A load blocks nothing but itself** (R6). The orchestrator holds one load
  lock *per model*, not per host: a predict to a model that is already
  resident takes no load-path lock at all, so it is never delayed by another
  model's load. It used to be — a single manager-wide lock was taken at the
  top of every predict, and run1 measured an 11.865 s load stalling every
  in-flight predict on the host for 11.885–11.894 s, 100.2 % of the load and
  28× the p50, with the 600 s deadline above as the worst case (finding
  P5-3/B18). What is still serialized, and why: two callers must not spawn the
  same model twice (that model's own lock), and only
  `[inference_local] max_concurrent_loads` models — default **1** — may be
  streaming weights into *one board* at a time, which is what keeps the
  ledger's load reservation for that board covering a single incoming
  footprint. A replica set spanning several boards takes one permit per board
  in sorted key order; replicas whose board cannot be resolved share one
  bucket, so a host with no GPU inventory keeps a single host-wide load at a
  time exactly as before.
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
- Every fatal path — whichever request was on the wire — reaps the child and
  records one **death report** before the error is returned: the worker
  label, the pid latched at spawn, the exit status, the terminating signal
  and core-dumped flag on Unix, what the orchestrator was doing, whether the
  gateway itself did the killing, and the stderr tail. It is logged at WARN
  by the supervisor itself, so a death that answers no request still leaves
  a line.
- **Who sent the signal.** The fatal path SIGKILLs the process group on
  every route into it, so a deadline timeout, a desynchronized stream or an
  unacknowledged unload reaches a *live* worker and reaps it as `signal: 9`
  — the same shape a kernel OOM kill takes. The report therefore carries an
  **attribution**, sampled immediately before the signal, with three states:
  - `still_running` — the child was alive and its stdout still open: the
    signal in the status is the gateway's own and says nothing about why.
  - `reaped_before_signal` — `try_wait` already had the exit status: the
    status is how the worker really died.
  - `dying` — the child was on its way down but not yet reapable. This one
    exists because `waitpid(WNOHANG)` does **not** report a thread-group
    leader while any thread of the group is alive, and a CUDA worker's
    driver threads take hundreds of milliseconds (475 ms measured) to unwind
    a SIGKILL. Reached from two facts the gateway can check without waiting:
    the worker's stdout is already at EOF (a live worker never closes it),
    or on Linux `/proc/<pid>/stat` shows the leader as a zombie. Before this
    state existed, the boolean it replaces reported exactly these deaths as
    the gateway's own doing (F12).

  Both outside states mean a `signal: 9` came from elsewhere — the kernel's
  OOM killer, the driver, an operator. The WARN line carries the state as
  `attribution=…` and keeps a derived `killed_by_gateway=` boolean, which is
  true only for `still_running`.
  Deliberate kills that leave *no* death report — the `unload`/terminate/
  kill ladder, a failed handshake or configure, the whole-set teardown
  after one replica dies, a dropped in-flight window — announce themselves
  at INFO before signalling instead, so no gateway SIGKILL is anonymous.
- **Idle replicas are swept for liveness.** A worker's death is normally
  discovered by a request hitting EOF on its stdout, which never happens for
  a model nobody predicts against — so the manager's sweeper ticks each
  dispatcher, which `try_wait`s every replica in its free pool. A replica
  found already exited gets the same treatment as one that died mid-request
  (whole model down, dropped from every cache, next request reloads),
  except that it settles no window: it had none in flight, so it is a
  liveness fact and never a memory negative on a unified board. Busy
  replicas are deliberately not swept — their window discovers the death
  sooner and with the request context attached.

## Environment (spawn contract)

The orchestrator sets for every worker:

- One device-visibility variable, when device pinning is active (absent =
  default). Which one, and in what vocabulary, is decided by the resolved
  accelerator (docs/rocm-batch-calibration-parity.md, D2):
  - `CUDA_VISIBLE_DEVICES` — CUDA hosts. Normally a `GPU-…` board UUID; an
    unresolvable registry pin passes through as written.
  - `HIP_VISIBLE_DEVICES` — ROCm hosts, always **a device index or a comma
    list of indices** (HIP reads nothing else). A board key resolves to its
    row index; a numeric pin, or an all-numeric list, passes through
    **canonicalised** (`"00"` → `"0"`, `" 1 , 2 "` → `"1,2"`) even when it
    names no board this host enumerated, because HIP can act on it and the
    operator's intent survives. Only a **non-numeric** pin that matches no
    board key is *dropped* — in a HIP visibility variable it would match no
    device, hide every board and silently run the worker on the CPU — and
    then the variable is simply not written and the worker inherits the
    environment. `CUDA_VISIBLE_DEVICES` is deliberately *not* also set there:
    it is a HIP alias, and setting both is documented unintended-behaviour
    territory. `ROCR_VISIBLE_DEVICES` is never set — torch < 2.6 crashes at
    init when it is.

  Exactly one is written, and only when a pin resolved; a worker is never
  handed both. **MPS and CPU hosts get neither**, in any vocabulary: there is
  one Metal device and no variable that names it, and on a CPU host no device
  at all, so anything written could only *hide* something. Their ledger board
  keys still resolve, so budgets and load reservations work as on a pinned
  host.
- `PYTORCH_MPS_HIGH_WATERMARK_RATIO=1.0` and
  `PYTORCH_MPS_LOW_WATERMARK_RATIO=1.0` — MPS hosts only. The high one pins
  torch's MPS allocator ceiling to Metal's `recommendedMaxWorkingSetSize`,
  which is the figure the ledger budgets that board against, so the hard error
  fires at the boundary admission assumes instead of wherever the build's
  default sits (it has drifted across torch versions and can sit *above* 1.0,
  i.e. inside macOS's compression/swap regime, where nothing raises at all).
  The resulting error is a `RuntimeError` the OOM classifier recognises by
  text. The low one is pinned **with** it because torch asserts `high >= low`
  when the allocator initializes: an ambient user-set low above 1.0 would turn
  the high pin into a hard startup failure. It is also the watermark the
  allocator garbage-collects cached buffers at, which is why the peak
  approximation above is biased near the ceiling.
- `PANOPTIKON_DEVICE_PIN=<the same resolved pin>` — written beside the
  visibility variable whenever one is written, and only then. It says *the
  orchestrator placed this replica, on this device*, which the visibility
  variable cannot: an operator's ambient `CUDA_VISIBLE_DEVICES` is
  indistinguishable from ours in the child's environment and means the
  opposite. The worker uses it for one check — a replica we pinned whose
  runtime enumerates **no devices** has silently fallen back to the CPU, so
  it fails its load with an actionable error instead of serving results
  twenty times slower while being priced against a board. A host whose
  operator hid every device (`CUDA_VISIBLE_DEVICES=-1`, a scheduler's
  ambient restriction) carries no marker and is untouched.
- `PANOPTIKON_UNIFIED_GPU=<pci address>` — replicas pinned to a
  **unified-memory** board, which today means an AMD APU on ROCm
  (docs/unified-memory-admission.md, DP-5). The value is that board's PCI
  address in the same `dddd:bb:dd.f` lower-case form as `gpu_bdf`, and the
  worker counts GTT **only when it matches the board it independently
  resolved** for itself: the `"amdgpu-sysfs"` sample above, and the
  `"fdinfo"` per-process base (`drm-resident-gtt` / the deprecated
  `drm-memory-gtt` alias, alongside the VRAM pair). The address rather than a
  flag, because a pin is a *belief* about where a replica lands and the ROCm
  design's one load-bearing unverifiable is exactly that belief; a bare flag
  on a mis-enumerated host would make a worker that came up on a dGPU report
  GTT-inflated free memory under an authoritative label. Set **per replica**,
  not per host — on a dGPU+APU machine one model's replicas can sit on both
  kinds of board — and absent, never `0`, on a discrete board. A mismatch, an
  unparseable value or a board the worker cannot yet identify all read as
  absent, which is the discrete arithmetic and is conservative in both
  directions. MPS workers do not get it: there is one kind of board on a Mac
  and their tiers are unified by construction.
- `INFERIO_DEVICE=cpu` — hosts priced against **system RAM**, i.e. those whose
  resolved accelerator is `cpu` (docs/unified-memory-admission.md, backend C).
  It does two jobs off one statement. `inferio.impl.utils.get_device()` honours
  it before probing, which is what makes pricing and execution agree: that probe
  asks the *machine* (cuda → mps → cpu), while the orchestrator prices what the
  installed wheels and the configuration say, and on a host where those diverge
  — an `accelerator = "cpu"` Mac, a box with an NVIDIA card whose venv holds the
  CPU wheels — the model would otherwise run somewhere nothing budgeted a batch
  against. And `inferio_worker.memory` reads it as the signal that this
  replica's memory currency is host RAM (`"ram"`/`"rss"` above), which cannot be
  inferred from the absence of an accelerator without mispricing every
  remote-API worker on a GPU host. `cpu` is the only value defined; a worker
  that does not recognise the value warns and probes, so a future one cannot
  brick an older worker. Written even on a CPU host whose RAM statistics could
  not be read and which therefore has no ledger board at all: coherence does
  not depend on pricing having succeeded.
- `INFERIO_WORKER=1` — marker for impl code that wants to know.
- `PYTHONIOENCODING=utf-8` — keeps worker stderr valid UTF-8 (defense in
  depth; the orchestrator's stderr forwarder tolerates arbitrary bytes from
  native code regardless).
- Inherited: `DATA_FOLDER`, proxy vars, PATH. Nothing else is promised.

The worker runs `python -m inferio_worker` with no arguments; everything it
needs arrives in the handshake.
