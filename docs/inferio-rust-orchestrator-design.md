# Inferio Orchestrator in Rust — Design

Status: DRAFT for review — 2026-07-05

> **Layout note (2026-07-11, M1 restructure):** paths in this document
> reflect the pre-restructure layout it was written against:
> `src/inferio/...` is now `python/inferio/...`, `src/inferio_worker/` is now
> `python/inferio_worker/`, and the `gateway` crate is now `panoptikon/`.
> The legacy Python inference server described in §2 survives on the
> `python-legacy` branch.
Scope: port inferio's orchestration layer (model lifecycle, process supervision,
request routing, batching) to Rust. Inference itself stays in Python worker
processes running the existing `src/inferio/impl/` classes, unchanged.

---

## 1. Goals and non-goals

**Goals (hard requirements)**

- Preserve the core mission: inference on a regular user's PC without hogging
  VRAM. Models load on demand, stay warm while in use, and are evicted by
  LRU + TTL so the GPU returns to the user.
- Preserve the cache-key architecture: independent subsystems (search UI,
  batch jobs, third parties) each control lifecycle through their own
  cache key with separate LRU sizes and TTLs, without stepping on each other.
- Reuse the existing Python inference implementations (`src/inferio/impl/`,
  `./inferio_custom/`) as-is, including their on-demand dependency imports.
- Reuse the existing model registry format (`src/inferio/config/inference.toml`
  + user overrides in `config/inference/`, group→id config inheritance,
  mtime-based reload).
- Design for multi-GPU on one machine (multiple worker replicas per model),
  even though the first implementation runs one replica.
- Make the implicit-batching / VRAM-bound interaction explicit and controllable
  (see §6) instead of accidental.

**Non-goals (now)**

- Python-environment management by the Rust binary (uv-managed venvs,
  per-impl envs). The dev assumption stands: we run inside the repo with the
  panoptikon venv available and impl classes at a known source path. The
  design must not *preclude* env management later (§9), but we don't build it.
- Multi-machine distribution. The gateway's weighted multi-endpoint pool
  already covers "several inference servers"; each server is one orchestrator.
- Ray. Both scaling attempts (multi-server batch fan-out, Ray) failed to
  deliver utilization; the streaming pipeline + per-GPU replicas is the
  replacement strategy. The Ray code path is not ported.
- Wire-format redesign. The HTTP API may change eventually (interop with
  Python panoptikon is not a long-term constraint; only DB compatibility is),
  but Phase 1 keeps the existing API so the gateway client and web UI work
  unchanged. Deliberate additions only (§7).

---

## 2. What we're porting — the facts that matter

From the current implementation (verified 2026-07-05):

**Server (`src/inferio/`)**
- One OS process per loaded model (`process_model.py`), spawned with
  `multiprocessing.Process`, IPC over `multiprocessing.Pipe` with dataclass
  messages (load / predict / unload / response, request-id matched), a
  listener thread per model routing responses to per-request queues.
- `ModelManager` singleton (`manager.py`): `_models` (loaded instances),
  `_lru_caches[cache_key] = OrderedDict[inference_id, expiration]`,
  `_cache_key_map[inference_id] = set[cache_key]` for refcounting. Eviction
  when an LRU exceeds its size pops oldest; a model is actually unloaded only
  when its last cache-key reference disappears. TTL sweeper runs every 10 s.
  During predict, TTL is set to -1 (pinned), restored to the requested TTL
  after — a model can't expire mid-inference.
- Dynamic batching happens **worker-side**: the subprocess drains all pending
  predict messages from its pipe, merges up to `MAX_COMBINED_BATCH` (env,
  default 32) inputs into one `model.predict()`, splits results back per
  request, and falls back to per-request prediction if the merged batch fails.
- Inference is serialized per model process; concurrency across models comes
  from separate processes.
- API: `POST /predict/{group}/{id}`, `PUT /load/{group}/{id}` (all with
  `cache_key`, `lru_size`, `ttl_seconds` query params), `DELETE
  /cache/{key}[/{group}/{id}]`, `GET /cache`, `GET /cache/{key}`,
  `GET /metadata`. Predict input is multipart: `data` form field (JSON
  `{"inputs": [...]}`) + files named by integer index; output is JSON,
  `multipart/mixed`, or `application/octet-stream` depending on content,
  with base64-`__type__` escape for mixed cases.
- Startup: env loading, logging, signal handlers, `static_ffmpeg` paths,
  `cudnnsetup` (CUDA DLL/library path registration — needed by workers).

**Clients**
- Rust gateway jobs: `inferio_client.rs` + `inference_pool.rs` (smooth
  weighted round-robin, failover with exclude list, `load_model_all` /
  `unload_model_all`). Cache args: `("batch", lru=1, ttl=60)`.
- **Per-request payload in the streaming pipeline: one item's work units**,
  chunked at `batch_size` only when a single item has more units than
  `batch_size` (e.g. many-page PDFs). A single image is a batch-of-1 request.
  `batch_size` is enforced as a semaphore over *total in-flight work units
  across concurrent requests* (`run_chunked_inference`, extraction.rs:562).
  GPU batch formation therefore relies entirely on worker-side merging of
  temporally-close requests.
  - Corollary: today, effective GPU batch ≤ `MAX_COMBINED_BATCH` (32) no
    matter what the UI batch_size knob says. batch_size > 32 only raises
    in-flight units, never the GPU batch.
- Search (PQL preprocess): batch-of-1 predict per embedding query, cache
  key/lru/ttl from `EmbedArgs`; gateway keeps its own query→vector LRU in
  front, so inferio only sees cache misses.
- Web UI: `GET /metadata`, `GET /cache`, and eager `PUT /load` when the user
  selects an embedding model. No direct predicts.
- Cron preload loop (already Rust): re-`load`s configured embedding models
  with lru=setter-count, ttl=3600, renews at ttl-130.
- Python's `DistributedInferenceAPIClient` (weight-split batches, wait-all) is
  superseded by the gateway pool + streaming pipeline and is not ported.

---

## 3. Placement: a module in the gateway binary

The orchestrator becomes a Rust module/crate compiled into the gateway binary
(`gateway/src/inferio/` or a workspace crate), serving the existing
`/api/inference/*` routes **locally** when enabled, exactly like the local API
graduation for jobs/cron:

```toml
[upstreams.inference_local]        # gateway config (naming TBD)
enabled = true
python = ".venv/Scripts/python.exe"   # dev default; auto-detected if absent
impl_dirs = ["src/inferio/impl", "inferio_custom"]
config_dirs = ["src/inferio/config", "config/inference"]
```

- `enabled = false` (or unset) → the gateway proxies `/api/inference/*` to a
  configured upstream, exactly as today. Remote inference servers keep
  working; each of them is just another gateway running with
  `inference_local.enabled = true`.
- The existing multi-endpoint pool is untouched: "local" is simply one more
  upstream (loopback or in-process); weights/failover still apply.

**Launch modes (decided 2026-07-05).** Inferio is conceptually independent
but *part of panoptikon* — same binary, normally same process (exactly as the
Python combined launch works today). One binary, three modes:

- default: gateway with local inference enabled — one process;
- `panoptikon inferio` (subcommand): start ONLY the inference service, for a
  machine that just lends its GPU;
- `inference_local.enabled = false`: gateway without local inference,
  pointing at remote upstream(s).

Why in-process rather than a separate `inferiod` binary:

- Isolation is already provided where it matters — inference runs in child
  processes. The orchestrator itself is thin supervision logic; a crash there
  is a bug we'd want to take the gateway down loudly anyway.
- One binary is the release-story ambition (user runs *one* thing).
- The gateway already owns config, logging, shutdown, and the client pool.
- HTTP remains the *canonical* boundary: Phase 1 keeps the gateway's own
  extraction/search clients talking HTTP to loopback, zero client changes.
  An in-process fast path (trait `InferenceBackend` with `Http` and `Local`
  impls, skipping multipart encode/decode for local calls) is a later,
  optional optimization — it must not change semantics.

The subcommand covers every "separate inferio" need without a second build
artifact; nothing in the design assumes same-process regardless.

---

## 4. Worker processes and protocol

`multiprocessing.Pipe` cannot cross the Rust/Python boundary, so workers get a
new, deliberately boring transport:

**Harness.** A new **minimal, separate package** (`src/inferio_worker/`,
decided 2026-07-05 — NOT inside the existing `inferio` package, whose
`__init__` pulls in the FastAPI router; the harness's import footprint must
stay strictly known because it is exactly what interpreter prewarming saves).
It duplicates the two small utilities it needs (impl-class discovery,
cudnn setup) and matches the end state where a user machine has no Python
inferio at all. Entry: `python -m inferio_worker` (parameters via handshake
frame). The harness:

1. Imports only stdlib + msgpack + the tiny protocol module (no torch, no
   FastAPI, no panoptikon, no inferio).
2. Runs cudnn path setup (cheap, python-env-specific — belongs Python-side).
3. Receives resolved `impl_class` + config kwargs in the handshake. **Rust
   owns registry parsing** (decided): workers never read TOML — today's impl
   classes already receive config as kwargs and know nothing of its source.
   The Rust side re-resolves the registry at every spawn so a worker is
   always born on current config (mtime reload, same as `/metadata`).
   Workers *running* on since-changed config are left alone for now; a
   config-hash per worker makes stale-worker reaping easy to add later if
   wanted (see §11).
4. Discovers the impl class the same way `utils.get_impl_classes()` does
   today (built-in dir + custom dir, `IMPL_CLASS` attribute).
5. Loops: read frame → dispatch → write frame.

**Transport: length-prefixed msgpack frames over stdin/stdout.**
- Works identically on Windows and Unix, no ports/sockets to manage, worker
  lifetime is naturally tied to the pipe (parent death → EOF → worker exits).
- msgpack handles large binary payloads (images in, embeddings/serialized
  numpy out) without base64 overhead; `rmp-serde` on the Rust side,
  `msgpack` on the Python side.
- stderr is inherited/piped into gateway tracing with a per-worker prefix —
  worker logs and tracebacks land in the normal gateway log.
- One outstanding request per worker (matches today's serialized-per-model
  execution); the orchestrator owns all queuing. This keeps the protocol
  trivially correct — no interleaving, no request-id routing needed in the
  worker (we keep request ids in frames anyway for sanity checking).

**Messages** (v1):
- `handshake` (→): protocol version, impl_class, config kwargs, device hint.
  (←): ok / error(traceback), impl-reported metadata if any.
- `load` (→) / ok|error (←): calls `model.load()`. Separate from handshake so
  a future prewarm state exists between "process up" and "weights loaded".
- `predict {inputs: [{data, file?}, ...]}` (→) / `outputs` | error (←).
  Inputs/outputs byte-for-byte equivalent to today's `PredictionInput` /
  output list semantics.
- `unload` (→) / ok (←): calls `model.unload()`, then exits 0.
- `prewarm` (→, reserved for Phase 3): instantiate impl and call an optional
  `prepare()` classmethod (imports heavy deps, no weights). No-op if absent.
- `ping` (→) / `pong` (←): liveness + handshake timeout enforcement.

**Supervision (Rust side)**
- Spawn via configured python; set env (`CUDA_VISIBLE_DEVICES` for device
  pinning, config dirs, log level). Reuse the existing
  `process_tree::JobGuard` (Windows Job Object kill-on-close) so worker trees
  can never outlive the gateway — same mechanism already proven for the
  HTML-thumbnail browser.
- Graceful stop: `unload` frame with deadline → terminate → kill. Mirrors the
  Python terminate-then-kill ladder.
- Startup handshake deadline; a worker that dies or hangs during `load`
  surfaces the traceback in the load/predict HTTP error, like today.
- Unexpected worker exit → model marked unloaded, all queued requests for it
  fail with a clear error, LRU entries dropped. (Python's behavior on a dead
  process is murkier; we make it explicit.)

---

## 5. Model manager: port the semantics exactly

A single `Mutex<ManagerState>` + a tokio sweeper task, semantics copied from
`manager.py`:

- `models: HashMap<InferenceId, ModelEntry>` where `ModelEntry` owns a
  `WorkerSet` (Vec of worker handles — **len 1 in Phase 1**, the multi-GPU
  seam, §8) plus the per-model request queue (§6).
- `lru_caches: HashMap<CacheKey, IndexMap<InferenceId, Expiration>>` —
  insertion-ordered, `lru_size` enforced on insert, oldest evicted first.
- `cache_refs: HashMap<InferenceId, HashSet<CacheKey>>`; a model unloads only
  when the last cache-key reference is removed (LRU eviction, TTL expiry,
  explicit DELETE, or cache clear).
- TTL: `ttl_seconds >= 0` → now + ttl; `-1` → never. Predict pins the entry
  (-1) for the duration and restores the requested TTL afterwards — with a
  refcount rather than a bare set, so overlapping predicts through different
  cache keys can't unpin each other (fixes a latent Python race).
- Sweeper ticks every 10 s, expires entries, unloads unreferenced models.
- Load is idempotent per (model, cache_key): repeated `PUT /load` renews TTL
  and LRU position — the cron preload loop and UI eager-load depend on this.

Everything above is observable behavior the UI, gateway cron preload, PQL
search, and extraction jobs already rely on. No changes in Phase 1.

---

## 6. Batching: move it into the orchestrator, make the cap explicit

Worker-side drain-the-pipe batching worked, but it hides the two knobs that
matter (window and cap) inside an env var, and no client signal reaches it.
The orchestrator replaces it with **queue-and-drain at dispatch time**:

- Per model, requests land in a FIFO queue. When a worker (replica) becomes
  free, the dispatcher takes up to `effective_max_batch` queued work units,
  sends them as one `predict` frame, and splits outputs back per request.
- **No batching timer.** A single search query on an idle model dispatches
  immediately (zero added latency). Under load the queue is never empty when
  a worker frees, so batches form naturally — identical dynamics to today's
  drain-the-pipe, but with an explicit, observable cap. (A wait-window can be
  added later if profiling ever shows first-batch sparsity matters; the user's
  read is that it's fine, and the streaming pipeline keeps the queue fed.)
- Requests larger than `effective_max_batch` are split into sequential
  sub-batches by the orchestrator (worker never sees an oversized batch).
- Batch failure → fall back to per-request prediction (port of the existing
  fallback, it has saved real jobs from one poisoned input).

**The VRAM-bound problem, treated explicitly.** VRAM per (model, hardware) is
unknowable in principle; `batch_size` on the job UI is the user's manual
overflow lever. Today that signal never reaches inferio, and merging can
combine multiple clients' requests. The fix is to carry the signal:

- `POST /predict` gains an optional `max_batch` query param (additive,
  ignored by old servers). Predict-only: under the stateless per-merge cap
  rule there is no state a load-time cap could attach to.
- Extraction jobs pass their job `batch_size` as `max_batch`. Search passes
  nothing (batch-of-1 queries don't express a VRAM opinion).
- The cap is **stateless — computed per merge** (decided 2026-07-05): when
  the dispatcher drains the queue to form a batch, `effective_max_batch` =
  max over the explicit `max_batch` values of the queued requests; requests
  without one contribute no opinion. If no queued request carries an explicit
  cap, fall back to the model's `default_batch_size` from registry metadata,
  else a server default (config, replacing `MAX_COMBINED_BATCH`).
  - **max**, not min: a cap asserts a fact about the hardware+model pair, not
    about the requester — if the user decided N is safe, N is safe, and
    smaller requests are contained in a larger batch.
  - Cap-less requests must NOT contribute the registry default to the max.
    Otherwise OOM recovery breaks: a job re-run with batch_size 8 merged
    alongside one cap-less search single would re-inflate the batch to the
    default and reproduce the OOM. "No opinion" means exactly that; the
    default applies only when nothing in the window has an opinion.
  - Stateless beats remembering the last write: no stale cap lingering after
    a job ends, no per-model memory to reason about, and a client sending a
    silly-low cap (search passing 1) can never clobber anyone else's batches
    — so nobody has to remember that search "shouldn't" set it.
  - Known limit, accepted: the cap only acts while capped requests are
    queued. Search-only deployments (e.g. panoptikon.dev, always-loaded
    embedding models, no jobs ever) never send caps and get the registry
    default — their requests are singles, so merges are tiny regardless.
- This also fixes the silent `min(batch_size, 32)` ceiling: a user-set
  batch_size now *is* the GPU batch cap, not just an in-flight cap.
- Cross-client merging remains possible (that's what batching is), but is now
  bounded by a cap the user actually controls per model, which is the best
  available answer given VRAM unknowability. Job + search overlap can still
  momentarily exceed the *job's* intent by queued search singles; if that ever
  matters in practice we can reserve one slot of headroom, but it's noise
  compared to arbitrary-model VRAM variance.

`batch_size`'s *client-side* meaning (in-flight work-unit semaphore in the
streaming pipeline) is unchanged. One knob, two enforcement points, same
intuition: "how many units may be in flight / in a GPU batch at once."

---

## 7. HTTP API: wire-compatible, plus deliberate additions

Phase 1 serves the existing surface byte-compatibly (multipart predict with
integer-indexed files, JSON / multipart-mixed / octet-stream responses,
base64 `__type__` escape, `/metadata` shape with group inheritance and mtime
reload, `/cache` shapes). Rationale: the gateway client, PQL, cron preload,
and the web UI all keep working with zero changes, and parity is testable
request-by-request against the Python server.

Additions (all additive):
- `max_batch` on predict (§6; predict-only under the stateless cap rule).
- `GET /health`: orchestrator + per-worker liveness, loaded models, queue
  depths — the observability we never had.
- (Reserved) `prewarm` load mode (§8).

Deferred (allowed later, since API compat with Python panoptikon is not a
long-term constraint): msgpack request body option to skip multipart overhead,
richer error taxonomy, streaming responses. None justify breaking the UI now;
when the web client branches for the Rust server anyway, we revisit.

---

## 8. Designed-for, built-later

**Multi-GPU (one machine).** The seam is `WorkerSet`: a model entry owns N
replicas, each spawned with its own device pin (`CUDA_VISIBLE_DEVICES=i`).
The dispatcher already routes "next batch → next free worker", which *is*
data parallelism across replicas; no other component changes. Config:
per-model `replicas`/`devices` in the registry TOML (`config.replicas`,
mirroring how `ray_config` rode along), default 1. LRU/TTL treats the set as
one unit. Phase 1 hard-codes N=1 but all call sites go through `WorkerSet`.
Note: today's impl classes pick devices themselves via `get_device()`;
replica pinning via `CUDA_VISIBLE_DEVICES` at spawn works without touching
them (each process sees exactly one GPU), which is why it's the chosen
mechanism.

**Placement policy (open design space, deliberately deferred).** Same-model
replicas across GPUs is the right shape for a lone batch job, but mixed
workloads want *separation* instead: a batch job saturating GPU 0 while the
user's search embedding models live on GPU 1. There is no natural config
surface owning this today — but the basic policy needs none, because the
cache-key system already encodes workload class: jobs load through the
`batch` key, search through its own keys. The orchestrator can use key
provenance as a placement hint at load time: batch-key models prefer the
emptiest device; interactive-key models prefer a device not hosting
batch-key models. Per-model config pins override. VRAM-aware placement is
impossible a priori (model sizes unknowable), but *observed* per-process GPU
memory (NVML per-pid, after load) can inform placement/eviction later. None
of this is committed scope — it's recorded so the WorkerSet/device seam is
shaped to allow a pluggable placement policy, and because it's the first
concrete answer to "where would placement config even live."

**Distribution stays at the instance level (decided 2026-07-05).**
Considered and rejected: a single inferio instance spanning machines (remote
workers over sockets). The federation model — every machine runs a full
panoptikon; Core points at one or more inference upstreams via the weighted
pool; every node keeps working alone when peers vanish (the PC/laptop/NAS
scenario) — is strictly more flexible, has no privileged entrypoint to
place, and is far simpler to configure than any peer-to-peer worker fabric.
If cross-node "one pool of GPUs" scheduling is ever wanted, it belongs in
the client-pool/Core layer (smarter per-model routing across upstreams), not
in worker transport. This is also what settles the transport question:
workers are always direct children of their orchestrator, so framed msgpack
over stdio wins outright.

**Prewarming (policy decided 2026-07-05, protocol v2).** Measured reality:
process start + library imports dominate load latency, not weights. A
prewarmed worker has run the impl's optional `prepare()` classmethod (heavy
imports, no weights, no GPU allocation — RAM-only cost) and is parked until
claimed. Protocol v2 splits identity from configuration (handshake carries
impl_class only; `configure` instantiates at claim time) precisely so the
pool is keyed by **impl class**: one warm worker serves any model of its
family. Policy:

- **Master switch, default ON.** One warm worker per impl class, and the
  pool does NOT TTL out — its entire purpose is to be there when the loaded
  model has TTLed away. If prewarm is enabled, the RAM is considered spent.
- **Eager set (default):** the same selection logic as
  `preload_embedding_models` — search-usable embedding setters with data
  (text-embedding/clip, excluding `tclip/`) across the index DBs — mapped to
  impl classes. Computed at startup and refreshed on the existing minute
  tick. Gated per-DB by a new SystemConfig flag `prewarm_embedding_models`
  (default true; same enumerate-all-act-per-config pattern as cron, same
  Rust-only-field precedent as `continuous_filescan`; SearchUI settings
  surface later). This targets the real UX hole: the first embedding search
  after a restart, for users who don't run Panoptikon 24/7. Models held
  fully loaded by `preload_embedding_models` don't benefit (they never
  unload), but the class-level warm worker is what catches them after that
  preload is disabled or fails.
- **Lazy warm (default ON, own switch):** after a model of class C is loaded,
  keep one warm C worker for next time (respawn-on-claim). Exclusion:
  extraction jobs pass an explicit `prewarm=false` hint (additive query
  param on load/predict, like `max_batch`) so batch-only model families
  don't burn RAM on warm workers nobody is waiting for. Explicit hint beats
  cache-key matching (brittle).
- **Whitelist:** `always_warm = ["impl_class", ...]` in gateway config,
  warmed unconditionally at startup (the only eager mechanism available to
  the standalone `panoptikon inferio` mode, which may have no index DBs).
- Claiming: ping the parked worker first (it may have died while parked);
  on failure fall back to a fresh spawn. A failed `prepare()` is per-request
  and non-fatal — the worker is still usable, the later `load` just pays the
  imports.

This structure maps 1:1 onto future per-impl venvs: a prewarmed worker is
keyed by (env, impl-class) instead of (impl-class).

**Environment management (the long-term ambition).** Out of scope now, but
the design leaves exactly one seam: workers are spawned from a configured
`python` path with configured impl/config dirs. A future env manager (uv is
the obvious candidate: single static binary we can ship or download, fast
venv creation, per-impl requirement sets) only changes *how that python path
is produced* — per-impl env definitions resolving to interpreter paths,
cached venvs, disk-cost-aware sharing. Nothing else in the orchestrator,
protocol, or manager changes. Until then: dev assumption (repo venv) with
auto-detection and a config override.

---

## 9. What is explicitly dropped

- **Ray mode** (`src/inferio/inferio_ray/`) — dead end, superseded by
  replicas + streaming.
- **`DistributedInferenceAPIClient`** — superseded by the gateway pool.
- **Worker-side `MAX_COMBINED_BATCH` merging** — replaced by orchestrator
  batching (§6). Workers execute exactly the batch they're handed.
- **`multiprocessing` IPC** — replaced by the framed-stdio protocol.
- **In-process (non-isolated) inference mode** — every model runs process-
  isolated. This was already effectively true (Core imports inferio but
  models run in subprocesses), and the Rust/Python boundary makes it
  mandatory. `static_ffmpeg` setup moves to harness-side only for impls that
  need it (audio/whisper), noted during Phase 1 porting of the startup
  sequence.

---

## 10. Phasing

**Phase 1 — parity port (the actual milestone).**
Worker harness + framed protocol; spawn/supervise with JobGuard; model
manager (LRU/TTL/cache-keys, exact semantics); registry TOML parsing with
inheritance + reload; full wire-compatible HTTP surface served locally by the
gateway behind a config flag; orchestrator queue with dispatch-time batching
at the server-default cap. Gateway's own inference upstream points at
loopback. Exit criteria: existing extraction jobs, PQL search, UI model
selection, and cron preload run unmodified against the Rust orchestrator;
side-by-side parity tests against Python inferio for the endpoint matrix.

**Phase 2 — the batch cap.** `max_batch` param end-to-end (extraction jobs
pass job batch_size; effective-cap rule; health endpoint exposes it).
Removes the silent 32 ceiling.

**Phase 3 — replicas + prewarm.** `WorkerSet` N>1 with device pinning;
`prepare()` hook + prewarm pools.

**Phase 4 (future, separate design) — env management.** uv-managed envs,
per-impl requirements, installer story.

---

## 11. Decisions log (2026-07-05 review) and remaining opens

Resolved with the user:

1. **Transport**: framed msgpack over stdio. Workers are always direct
   children; distributed inference happens at the *instance* level
   (federation of full panoptikon nodes via the weighted pool), not by
   distributing one inferio across machines (§8).
2. **Registry ownership**: Rust parses the TOML; workers receive resolved
   `impl_class` + kwargs in the handshake and never read config. Registry is
   re-resolved at each spawn so workers are born on current config (§4).
3. **`effective_max_batch`**: stateless, per-merge `max()` over explicit caps
   in the drain window; cap-less requests contribute no opinion; registry
   default only when no request has an opinion (§6).
4. **Placement**: module in the panoptikon binary; normally same process;
   independently launchable via a `panoptikon inferio` subcommand; local
   inference disableable by config (§3).
5. **Harness layout**: minimal separate package (`src/inferio_worker/`),
   strictly-known import footprint, no dependency on the legacy `inferio`
   package (§4).

Still open (deferred by scope, recorded here so they aren't lost):

- **Stale-worker reaping**: whether/when to recycle workers whose config
  changed after spawn (config-hash per worker makes this cheap to add;
  today's Python behavior is equivalent to "never reap").
- **Multi-GPU placement policy**: cache-key-provenance heuristic + config
  pins sketched in §8; fixed-vs-dynamic policy, and any cross-node pooling,
  are future design work in the Core/pool layer.
