# Batch calibration and VRAM budgets — design

Package 2 of the GPU compatibility work: the concrete design for items 5–8
of `gpu-compatibility-design.md` (self-calibrating batch size, pixel-budget
admission, footprint recording, VRAM-aware behaviour). Decided 2026-07-30;
revised the same day after design review (grant ledger, dispatcher window
rule, profile-key fallback, tiered base measurement); second review pass the
same day (envelope fit, pool-aware ledger + idle-resident trim, grant dual
denomination, reset and residual questions settled); third review pass the
same day (WDDM throughput-collapse signal, extrapolation ratchet,
non-local-profile margins, dispatcher pricing declared estimate-only);
fourth review pass the same day (single-currency driver-MB ledger,
load-phase reservations, universal worker→GPU pinning, free-intercept
fit); fifth review pass the same day (persisted ratchet state, local-store
write policy, pre-fit WDDM comparator, dtype-unknown load reservations,
concrete per-DB migration mechanics). Supersedes the one-line itemization
in that document. Not yet implemented.

## Core decision: learn a cost model, not a max batch size

Calibration does **not** learn "the batch size that fits". It learns a
per-model **memory cost model**

```
memory ≈ base + slope × units
```

where `base` is the load footprint (weights + fixed overhead), `slope` is
the marginal cost per unit of input, and *unit* is a model-specific cost
dimension declared in the model's metadata. Every batch is then sized
against the **currently available budget**: read live free memory, account
for every claimant, divide the remainder by `slope`, pack inputs up to that
many units.

Why this and not a learned max batch:

- A max batch bakes in the free memory at learning time. On desktops other
  processes' VRAM usage changes constantly, including mid-job; a cost model
  is re-evaluated against *live* free memory before every batch.
- A max batch bakes in the input composition it was tried with. For models
  whose memory scales with input size, one large image moves the number
  wildly; a cost model prices the actual batch being assembled.
- A max batch is per-card-capacity. A cost model is shared by every card of
  the same GPU model: a 12 GB and a 6 GB variant have identical slopes and
  different budgets. This is what makes profiles shippable.
- Learning a max batch means probing until OOM. On a desktop with a GUI on
  the GPU that is exactly the experience we must not create. The cost model
  is fitted from *measurements of our own usage at safe sizes* and
  extrapolated — the OOM boundary is predicted, never sought. The Package-1
  OOM halving loop remains as a backstop for prediction error and
  external-usage races, not as the mechanism.

OOM is not a reliable signal (anything can be using the GPU); our own
measured usage is. All calibration derives from the latter.

"Ideal" is bounded by a second observation: some models stop gaining (or
lose) throughput past a certain batch size. Calibration therefore also
records items/sec per tried size and caps at the **throughput knee** even
when memory would allow more.

## Cost dimension taxonomy

A model's cost dimension is `(unit, aggregation)`:

- `unit`: `item` | `pixel` | `token` | `audio-second`
- `aggregation`: how per-input units combine into batch units:
  - `count` — batch units = number of items (unit is fixed-size per item)
  - `sum` — batch units = Σ per-item units (e.g. total decoded pixels)
  - `max-times-count` — batch units = (largest item's units) × item count
    (padded/uniform batches: every slot pays for the largest member)
- `none` — no meaningful GPU batch scaling (remote APIs, sequential
  engines). No admission; at most a `base` footprint is recorded.

Declared in `inference.toml` metadata per group, overridable per inference
ID (same layering as every other metadata key). Missing declaration
degrades to `(item, count)` with a conservative slope — worse packing,
never a crash.

### Classification of the shipped registry

| impl_class (group) | dimension | basis | status |
|---|---|---|---|
| `wd_tagger` (tags) | `item` / `count` | model's own preprocess transform resizes to a fixed square (448px class) | verified in code |
| `moondream_tagger`, `moondream_captioner` (tags, vlm) | `pixel` / `sum` | variable-resolution VLM with internal tiling; effective resolution bounded by the model's own cap | **verify at impl time** — if the internal cap is low, `item`/`count` may fit better |
| `danbooru_tagger` (tagmatch) | `none` | network lookups, `num_gpus = 0` | verified in config |
| `dotsocr` (doctr) | `pixel` / `sum` | variable-resolution VLM; image-token count (and the KV cache behind `max_new_tokens = 128`) scales with decoded pixels | verified in code (dtype/FA2 sites) |
| `easyocr` (doctr) | `pixel` / `max-times-count` | batched CRAFT path requires uniform dims and pays max-size × batch — the known OOM trap ([easyocr-batch-oom]); currently `enable_batching = false` stopgap | verified in code |
| `doctr` (doctr) | `item` / `count` | detection resizes to a fixed canvas (1024²-class); recognition crops are tiny and data-dependent (text density) — absorbed by margin | **verify at impl time** (recognition variance) |
| `florence2` | `item` / `count` | processor resizes to fixed 768×768; generation budget fixed per task prompt | verified in code |
| `sentence_transformers` (textembed) | `token` / `max-times-count` | inputs pre-split at `max_seq_length`, then padded per batch to the longest member | verified in code |
| `jina-clip-api` (textembed, clip, tclip) | `none` | remote API | verified in config |
| `faster_whisper` (whisper) | `none` | CT2 processes 30 s windows sequentially; VRAM ≈ constant per model, no torch allocator to measure. Excluded from calibration v1 (as it is from `run_with_oom_retry`) | verified in code |
| `openclip` (clip, tclip) | `item` / `count` | fixed preprocess resolution per model (224/378/384px) | verified in code |
| `qwen3-vl-embedding` (clip, tclip) | `pixel` / `sum` | qwen-vl-utils variable-resolution path (processor-capped) | **verify at impl time** (confirm processor pixel cap) |
| `clap` | `item` / `count` | ClapProcessor pads/truncates audio to a fixed window | **verify at impl time** |

Notes:

- `max-times-count` models benefit most from **bucketing**: packing sorts
  the pending window by per-item units and builds batches from
  similarly-sized neighbours, so one 8000×6000 scan doesn't tax 63
  thumbnails. This is what finally retires easyOCR's
  `enable_batching = false`. Safety never depends on bucketing —
  max×count pricing admits a mixed batch conservatively (one big scan →
  batch of 1–2) — it is purely the throughput win, and its depth comes
  from window sizing (below). The easyOCR acceptance test must run under
  realistic core pipelining, or it measures a depth that never occurs in
  production.
- For `pixel` units, "units" means decoded pixels *as submitted* (after
  input-spec slicing/downscale) — the same quantity
  `slice_settings.mode = "pixels"` already reasons about upstream.
- Backends without a free-memory query (MPS, CPU) degrade to no
  admission: seed-sized fixed batches plus the Package-1 backstop, the
  same class as `none`.
- A resident `none`-class worker with no torch allocator (faster_whisper /
  CT2) reports ~0 `memory_reserved`, so its real VRAM lands in the
  ledger's `external` term — margin-inflated but safe. This is the
  intended accounting, not phantom headroom, until CT2 footprint
  recording exists (see Open questions).

## Where each piece runs

The inference server (inferio) is independent of core, can be remote, and
can serve several cores; one host can run several workers (models ×
replicas) on one GPU. VRAM is therefore a shared resource with exactly one
component that sees all claimants — the Rust orchestrator — and sizing
must be centralized there, not computed independently per worker:

- **The orchestrator is the budget arbiter.** Per GPU (by board UUID) it
  keeps a ledger: the configured limits, each resident worker's recorded
  `base`, each in-flight window's outstanding **grant**, and the freshest
  external-usage sample. All sizing intelligence lives here: it fits the
  cost model from reported samples, owns persistence (atomic TOML rewrite
  of the local store, shipped-baseline loading), sizes dispatcher windows,
  attaches a memory grant to every window, and exposes state read-only
  over the API for UI/labels (`/api/inference/metadata` overlay, like
  `unavailable` today).
- **The worker is mechanism and sensor.** It is the only place that has
  torch, the allocator statistics, `mem_get_info`, and per-item unit
  counts after decode. A harness around `predict()` (sibling to
  `run_with_oom_retry`) packs the window into GPU batches within the
  grant (bucketed for `max-times-count`), applies the defensive clamp,
  measures every batch, and reports measurements plus a fresh device
  memory sample on the response frame.
- **Core keeps its opaque-ID worldview** — it learns nothing about VRAM,
  GPUs, or profiles. It forwards the user cap per request and sizes its
  *requests* by its own concerns (payload memory in flight, pipelining),
  not by guessing GPU batches. The orchestrator/worker re-slices whatever
  arrives.

Transport: **no new channels**. The worker protocol is strictly
request/response with one window in flight per worker, so grants (and
cost-fit snapshots, when they change) ride on request frames,
measurements and memory samples ride on response frames, and the load
response carries the base measurement. A worker-initiated query channel
("ask the scheduler for the current budget mid-window") was considered
and rejected: it complicates the protocol for a grow-direction freshness
win the grant model already bounds (see the staleness note below). One
addition ships in v1 and is compatible with that rejection: an
**orchestrator-initiated trim message** (see Reactive shrink below) — a
new message type on the existing request/response channel, same direction
as `load`; only worker-initiated queries were rejected.

Two keyspaces, deliberately different:

- **Cost profiles** are keyed by GPU *model* (`name` string) + environment
  tuple — a property of the silicon and software, shareable.
- **Budgets and budget settings** are keyed by GPU *instance* (board UUID,
  `GPU-…` from NVML/nvidia-smi/torch device properties). Two identical
  cards on one host share profiles but can carry different budget settings
  (e.g. the one driving the monitors gets a bigger margin). CUDA device
  index is **never** an identity — it is not stable across reboots or
  `CUDA_VISIBLE_DEVICES` changes.

**Every worker is pinned to exactly one GPU.** The spawn machinery
already supports pins (`config.replicas`/`config.devices` →
`CUDA_VISIBLE_DEVICES` per replica), but the default today is a single
*unpinned* replica that sees every device — impls then run on
`devices[0]`, so on a multi-GPU host attribution is ambiguous and card 0
silently gets everything. Under the ledger, ambiguity is unacceptable:
the orchestrator resolves an explicit pin for every worker at spawn,
written in the UUID form CUDA accepts directly
(`CUDA_VISIBLE_DEVICES=GPU-…`), so the pin shares the budget keyspace's
identity and device-index instability never enters (ROCm may need the
index form plus a spawn-time index→UUID mapping; cuda-first as
everywhere). Default placement keeps today's behaviour — first usable
GPU; headroom-based placement across cards is a natural later upgrade
once ledgers exist, not v1. The impl-side multi-device path
(`get_device()` returning several devices) drops out of the supported
envelope: a worker sees exactly one GPU, and every report (base,
reserved, memory samples) lands on exactly one ledger.

## Dispatcher windows and the batch cap

The dispatcher's current effective-cap rule (max over the explicit
`max_batch` values in the window; else registry `default_batch_size`;
else server default) existed to reconcile "inferio doesn't know what is
safe" with heterogeneously-capped requests. Grant-based admission removes
that job, so the rule is **deleted, not adapted** — its OOM-recovery
rationale is obsolete once safety lives in the ledger.

Under auto:

- **Window size comes from the orchestrator's fitted model**, like the
  grant: a few GPU batches' worth of units (≈2–4× the current admitted
  batch estimate; seed-derived before calibration), additionally bounded
  by payload bytes (the 512 MiB frame limit is the hard wall). Windows
  deep enough to hold several batches are what give bucketing material
  and amortize the request/response round trip; the *bound* is what keeps
  work divisible across replicas (an unbounded drain would hand the whole
  queue to the first free replica) and keeps the failure blast radius
  small (a window is the unit of fallback and of fatal-error loss). There
  is no time bound anywhere: `predict` keeps its no-deadline semantics.
- **Dispatcher-side unit counts are estimates, and safety never depends
  on them.** Window sizing and grant pricing need per-item units before
  any worker has decoded anything: `pixel` models use image-header
  dimensions (parsed at dispatch, or forwarded by core, which already
  knows post-slicing dims); `token` models use a bytes-per-token
  heuristic (the dispatcher cannot tokenize); `max-times-count` window
  depth uses the sum-of-units approximation (true max×count is undefined
  before the worker buckets). Mis-estimates only mis-size windows — an
  over-estimate yields a larger grant still clamped by headroom, an
  under-estimate yields more GPU batches per window — because the worker
  packs within the grant using exact post-decode counts.
- **The user cap travels per request.** Windows are partitioned by cap
  value — capped jobs are the exception under auto, so mixed-cap queues
  are rare and the partition costs nothing — and the worker enforces the
  cap at pack time as an **item-count constraint**, never converted to
  units.

## Grant sizing and packing

Orchestrator, per GPU, when dispatching a window:

```
footprint(w) = base(w) + max(0, reserved(w) − reserved_at_load(w))
                                                 # driver currency, ≥ base
external  = max(0, total − free − Σ footprint(our workers))
limit     = min(total × cap_fraction,           # server lever, default off
                total − external × (1 + margin)) # desktop lever, default on
headroom  = limit − Σ footprint(residents) − Σ load_reservations
                  − Σ outstanding_grants
grant     = min(headroom share, ramp step, slope × knee_units,
                priced content of the window itself)
```

- **The ledger runs in one currency: driver MB.** A worker's charge is
  its `footprint` — process-level `base` (context + workspaces +
  weights) plus allocator pool growth since load. Charging allocator
  `reserved` alone would misclassify each resident's ~0.5 GB context and
  workspaces as *external* (margin-inflated) while `base` counts them
  again — a systematic double-count worth 1.5–2 GB across a few
  residents; charging `base` alone would hand a resident's retained pool
  out again to neighbours — releasing a grant returns nothing physically
  until `empty_cache()` — who then hit the defensive clamp forever.
  `footprint ≥ base` by construction: residency changes who has already
  paid the base, not whether it counts. Where NVML per-process works the
  orchestrator may substitute the exact per-PID figure (the same tier
  machinery as base measurement); `base + pool growth` is the WDDM-safe
  approximation.
- **A grant is dual-denominated**: an MB reservation (the ledger
  currency) and a unit budget (the packing currency). Post-fit the unit
  budget derives from the MB side via the slope; pre-fit there is no
  slope, so the unit budget is the ramp value (`seed_units × 2^k`) and
  the MB side is the contention share held while that step is measured.
  Without this the ramp is unit-shaped, the ledger is MB-shaped, and the
  conversion is undefined exactly when it is needed most.
- **Grants are reservations, not estimates.** Two replicas cannot claim
  the same headroom, so the concurrent-ramp race is structurally
  impossible rather than probabilistically mitigated. A grant is released
  when its response frame lands; a dying worker's grants are released
  with its aborted windows under the existing generation guard. A *hung*
  worker (stuck CUDA call) holds its grant indefinitely — deliberate:
  `predict` has no deadline by standing policy, the memory genuinely is
  unavailable, and the contention floors keep neighbours running at
  seed-batch throughput until the operator intervenes (drain + restart,
  the existing stuck-CUDA recovery).
- **A load in progress is a reservation too.** Loads are serialized by
  the manager's load lock, but dispatch is concurrent with loading:
  without a charge, windows granted during a multi-second load collide
  with the incoming weights. From load-start the ledger holds a
  `load_reservation` at the *expected* base (local profile → shipped
  profile → conservative constant), replaced by the measured value when
  the load response lands. This is also item 8's trigger arriving early:
  expected base exceeding headroom is the evict-before-load signal.
  One wrinkle: `dtype` is in the profile key, but dtype negotiation
  (Package 1) resolves *during* the load — on the first-ever load of a
  model on a GPU the orchestrator cannot know which dtype's profile to
  consult, and guessing fp16 when negotiation lands on fp32
  under-reserves ~2× for exactly the seconds the reservation exists to
  protect. When the negotiated dtype is unknown, reserve at the most
  conservative plausible dtype's base (fp32 profile if present, else the
  constant); the load response reports the actual dtype, and the
  orchestrator remembers the negotiated outcome per (model, GPU) for
  subsequent loads.
- **External usage is derived, not margin-guessed, for our own
  processes.** Every worker reports `memory_reserved` per response (and
  `reserved_at_load` once, on the load response), so the orchestrator
  computes footprints and the margin multiplier applies only to
  genuinely external usage — sibling workers, contexts and workspaces
  included, are never margin-inflated. `external` is clamped at ≥ 0:
  `free` and the per-worker samples come from different moments, and
  sampling skew must never manufacture phantom headroom. Samples arrive
  only on response frames, so after a long idle gap the first window
  prices `external` from a stale sample; the shrink clamp makes that
  safe, and the orchestrator refreshes via NVML (the Package-1 probe
  machinery) when the freshest sample exceeds an age threshold — a
  single coherent snapshot (total/free/per-process in one read),
  preferred over stitched per-frame samples whenever it is fresh. In
  scope for v1, since the probe machinery already exists; an accuracy
  measure, not a safety requirement.
- **Contention policy** when several models are hungry at once: demand
  first (queue depth; an idle model consumes no new grants, though it
  holds its pool until trimmed — see Reactive shrink), then split by
  calibrated appetite `slope × knee_units`, falling back to `base`
  weighting before calibration, with a floor of one seed batch per worker
  so nothing starves to zero. When even the floors oversubscribe
  headroom they shrink pro-rata — grants are reservations and the ledger
  invariant is never violated — bottoming out at the one-item minimum at
  pack time.
- **Fit confidence widens margins automatically**: `residual_mb` (and
  non-local-profile status — any shipped or fallback-matched entry not
  yet locally confirmed, see Lookup) inflate that model's effective
  margin, clamped to a maximum factor. Safety never depends on a human reading a
  Desktop label; the future tab's "verified" badge is presentation on
  top of the same number.
- **Ramp**: until the fit has enough samples, grants ramp geometrically
  (seed, ×2 per clean window) instead of jumping to the predicted
  ceiling, measuring each step. A too-low seed costs a logarithmic number
  of windows, which is why seeds don't need per-GPU tuning.
- **Extrapolation ratchet**: the ramp never ends by handing control to
  extrapolation. Even after the fit converges, a grant's unit budget
  never exceeds ~2× the largest *locally measured* clean high-water
  batch; the measured range extends itself geometrically under real
  load. The fitted model's job is pricing mixed compositions and
  re-evaluating against live free memory — never predicting far beyond
  evidence, which is exactly where nonlinear effects (allocator
  behaviour, attention memory, workspace growth) break linearity, and
  where WDDM gives no clean failure (see Backstop). The ratchet counts
  only local samples, so a fresh install ramps from seed even with a
  shipped profile: profiles govern pricing, `base` accounting, and the
  knee cap — not growth. The ratchet anchor **persists**: the local
  store records the largest locally measured clean high-water batch
  (see Calibration store), so a restart resumes from the measured range
  instead of re-ramping from seed — otherwise the "ramp cost is
  logarithmic and one-time" argument silently becomes "per restart" on
  desktops. A persisted anchor still enters every window through the
  defensive clamp against live free memory, and deflation state remains
  runtime-only.

Worker, per batch within its window:

- Pack up to the grant's unit budget (bucketed for `max-times-count`);
  a batch is never smaller than one item — a single item over budget
  goes through anyway (the backstop catches it if it truly cannot run;
  Package 1 already decided batch-1 OOM = item fails, job continues).
  Bucketed packing reorders items; the worker restores input order
  before replying, since the dispatcher splits outputs back per request
  by position.
- **Defensive clamp, shrink-only**: before each batch, check live
  `mem_get_info` and pack smaller if the world moved; never exceed the
  grant. Freshness is therefore per-batch in the shrink direction and
  per-window in the grow direction — staleness can only *under*-size
  (memory freed mid-window is not seen until the next window's grant), a
  throughput nibble bounded by window depth, never a safety issue.
- **Measurement**: the fit runs orchestrator-side in **reserved**
  currency — that is what the driver (and therefore the budget) sees;
  allocator fragmentation and library workspaces make `allocated` a
  systematic underestimate, not scatter. But reserved is an **envelope,
  not a per-batch delta**: the caching allocator never returns blocks
  between batches, so once the pool covers the working set a repeat
  batch grows reserved by zero, and a delta series drags the fitted
  slope toward zero — over-admission, the exact failure this design
  exists to prevent. Only **high-water batches** — those that grow the
  pool: every geometric ramp step, and regrowth after `empty_cache()` —
  contribute reserved samples, regressing `peak_reserved −
  reserved_at_load` against batch units with a **free intercept**:
  `base` is process-level driver currency the allocator never saw, so
  forcing the fit through it (or through zero) would bias the slope low
  — admission uses the slope; the intercept is diagnostic only.
  Warm-pool batches contribute the allocated
  transient (`peak_allocated − allocated_before`, which has no caching
  hysteresis) as the diagnostic floor and validation series.
  `empty_cache()` events are therefore calibration opportunities, not
  just hygiene. Robust two-parameter fit; retain scatter (sample count,
  residual) as confidence.
- **Reactive shrink**: grants shrink as external usage rises, but freeing
  our tensors is not enough to give memory *back* — the allocator pool
  holds it — so when the grant falls materially below `memory_reserved()`
  (hysteresis: e.g. below 80% of pool for 2 consecutive windows), call
  `empty_cache()` between batches. Exact thresholds: implementation
  detail, tune empirically.
- **Trim for idle residents**: the reactive-shrink path only runs in
  workers that are receiving windows — an idle resident gets no frames,
  so its retained pool would squeeze its neighbours indefinitely. When
  the ledger sees a hungry worker constrained by an idle resident's
  pool slack (`reserved − reserved_at_load`, the growth term of its
  footprint), the orchestrator sends that resident a trim
  request; the worker calls `empty_cache()` and replies with a fresh
  memory sample. Trim is not unload: it releases only pool slack —
  weights, live tensors, and the CUDA context stay, so the model remains
  resident at a cost of milliseconds plus re-`cudaMalloc` as the pool
  regrows — whereas unload (item-8 eviction) frees `base` too at full
  reload cost. Trim when budgets are tight; evict when even the bases
  don't fit.
- **Backstop**: `run_with_oom_retry` unchanged. An OOM despite admission
  is recorded as a negative sample (prediction was wrong or the world
  moved) and deflates that worker's grants; N consecutive clean windows
  restore them — deflation must be recoverable, or one external spike
  degrades a worker until respawn. Deflation is runtime state,
  deliberately not persisted across restarts.
- **WDDM synthetic negative sample**: on Windows the OOM signal is
  unreliable by construction — driver sysmem fallback (default on since
  ~536) lets an over-budget allocation succeed by spilling to system
  RAM, so over-admission usually manifests as a silent throughput
  collapse, never an exception, and the OOM path above would simply not
  fire. The worker already times every batch for knee capture; a
  pool-growing batch whose units/sec craters far below the fitted
  throughput curve is therefore recorded as a synthetic negative sample
  feeding the same deflation path. **Pre-fit the comparator is the
  previous ramp step**: a ×2 units step whose units/sec drops by the
  collapse ratio relative to the prior step is a spill — without this
  the ramp, the riskiest phase (especially under a wrong shipped
  profile), would be exactly the window the signal cannot cover. No new
  machinery — it reuses the timing and the deflation mechanism. Collapse
  threshold: implementation detail, tune empirically. Corollary at
  batch 1: a single over-budget item "goes through anyway" and on WDDM
  it does not fail as it would under Package 1's batch-1-OOM rule — it
  silently runs slow, once. Accepted: `slice_settings` bounds decoded
  pixels upstream, and it is one item, not a regime. Documentation (and later the Desktop tab)
  should additionally recommend the driver's "Prefer No Sysmem
  Fallback" setting (NVIDIA control panel, driver ≥ 546; not settable
  programmatically) — with it, Windows regains a crisp OOM signal and
  the synthetic path becomes the fallback for default-configured
  machines rather than the primary signal.

The only timing assumption left: external usage doesn't swing by more
than the margin within one window. The backstop covers the exceptions.

## Base measurement

`base` is the worker's whole-**process** device footprint, not its
allocator footprint: the CUDA context (~300–600 MB) and cuDNN/cuBLAS
workspaces reduce free memory but never appear in allocator statistics,
and the ledger (and item-8 eviction) count residents in driver currency.
Undercounting each resident by half a GB, times several residents, is
phantom headroom. Measurement is tiered:

1. **NVML per-process** (`nvmlDeviceGetComputeRunningProcesses`, own
   PID's `usedGpuMemory`) — exact and pollution-free; reliable on Linux
   including the CUDA Docker image under the nvidia runtime
   (`nvidia-ml-py`, pure-Python dependency).
2. Where NVML reports N/A — **Windows WDDM** — fall back to the
   `mem_get_info` free-delta around load, clamped to ≥ the
   allocated-delta. It is a one-shot sample: a reading implausibly larger
   than allocated means another process moved during the load window →
   fall back to allocated + a fixed context estimate.
3. The `max_memory_allocated` delta around load is always recorded as the
   floor.

`base_method` is recorded in the profile as provenance. Cross-platform
contamination is impossible by construction: `platform` is in the profile
key, so Linux bases (exact, with Linux-sized contexts) never overlay
Windows entries, whose WDDM contexts are genuinely different sizes.

## Budget configuration

Per-server defaults with per-GPU-instance overrides, in the inferio side of
the server config. Two composable limits; when both are set the admission
budget is the `min`:

```toml
[inference_local.vram]
# margin over other processes' usage; the desktop lever, default on.
# usable = total − other_used × (1 + margin)
# margin = 0.10

# hard ceiling as a fraction of total VRAM; the server lever, default off.
# cap_fraction = 0.90

[inference_local.vram.gpu."GPU-1a2b3c4d-....."]   # board UUID, per instance
# margin = 0.25          # e.g. the card driving the monitors
```

Defaults live in serde defaults per the config-authoring rules; the TOML
ships commented examples only. `margin` defaults on (0.10) everywhere —
on a headless server other-usage is ~0 so it costs nothing; server
operators who partition VRAM among services set `cap_fraction` and are
encouraged to leave `margin` alone.

## Calibration store

### File format

Human-readable TOML, one array-of-tables; the same file format for shipped
baselines and locally generated data.

```toml
schema = 1

[[profile]]
inference_id = "clip/ViT-H-14-378-quickgelu_dfn5b"
epoch        = 1                       # from model metadata; stale-epoch entries are ignored
gpu          = "NVIDIA GeForce RTX 5090"
platform     = "windows"               # windows | linux | macos
backend      = "cuda"                  # accelerator extra (cuda | rocm | cpu)
torch        = "2.7.1+cu128"
dtype        = "fp16"                  # negotiated dtype actually in use
unit         = "item"                  # denormalized from metadata for readability
aggregation  = "count"

base_mb           = 4321               # load footprint (process-level, see Base measurement)
base_method       = "nvml"             # nvml | free_delta | alloc_delta
slope_kb_per_unit = 812.5              # marginal cost, fitted on reserved deltas
knee_units        = 512                # optional: throughput stopped improving here
samples           = 38
residual_mb       = 96                 # fit scatter → confidence / safety margin
measured_at       = "2026-07-30T00:00:00Z"
generator         = "panoptikon 0.1.8" # provenance

# Local-store-only fields (ignored when read from a shipped baseline —
# they carry local authority a foreign measurement cannot):
max_units_measured = 1024              # ratchet anchor: largest locally
                                       # measured clean high-water batch
local_samples      = 12                # local clean samples; also the
                                       # non-local-profile confirmation gate
```

Key tuple for lookup: `(inference_id, epoch, gpu, platform, backend,
torch, dtype)`.

**Lookup is a fallback hierarchy, not an exact match** on the full tuple:
exact torch string → same torch `major.minor` ignoring the local version
tag (`backend` already encodes the CUDA/ROCm family) → no match. The full
string stays in the file as provenance; `epoch` remains the deliberate
invalidation lever. Without the hierarchy, every torch patch bump would
orphan the entire shipped-baseline set, and volunteers on different
patch versions would produce disjoint, never-matching entries.

**Any profile not generated locally — shipped baselines included, even
on an exact tuple match — is used with a widened effective margin until
a few local clean samples confirm it.** Driver version is deliberately
not in the key, and `base` is driver-currency, so a foreign measurement
is a good prior, never ground truth; fallback-matching is just the
least-confident case of this one rule. The cost is a conservative first
few windows on a fresh install (confirmation is a sample-count gate,
and local samples accrue on every ramp step), largely masked by the
ramp, which governs growth regardless (see the extrapolation ratchet).

### Layering and lifecycle

- **Shipped baselines**: `python/inferio/config/calibration/*.toml`,
  beside the model registry, mtime-reloaded the same way, **not**
  user-seeded (per the CLIP-FP16 lesson: `python/inferio/config/` is not a
  user-owned surface). Populated over time from maintainers' and
  volunteers' generated files.
- **Local store**: one generated TOML in inferio's data directory, written
  by the orchestrator, overlays shipped entries (local wins on identical
  key). Deleting an entry (by hand or from a future Desktop surface)
  triggers recalibration — passively, on the next run.
- **Write policy**: the orchestrator updates a local entry (via the
  atomic rewrite) whenever the ratchet anchor advances or the fit
  meaningfully changes — not per batch. This is what makes the ratchet
  and the confirmation gate survive restarts; runtime-only state
  (deflation, ramp position within a step, outstanding grants) is
  deliberately never persisted. Because a shipped baseline's `samples`
  are the *generator's*, local confirmation always reads
  `local_samples` — a shipped entry confirms only by accruing a local
  overlay entry.
- **Calibration is never frozen**: the fit keeps ingesting qualifying
  samples for as long as the model runs — high-water batches from
  ratchet range extensions and post-`empty_cache()` regrowth (shrink and
  trim events are calibration opportunities), warm-pool transients as
  the validation series, throughput samples for the knee. To make
  continuous refitting survive restarts, the local entry also persists a
  **bounded ring of recent high-water samples** (`(units, reserved_mb)`
  pairs; local-only like the ratchet fields, stripped on
  baseline import) — a robust fit cannot be resumed from aggregates
  alone, and ring eviction doubles as recency aging: samples from a
  since-changed driver or allocator fall out instead of anchoring the
  fit forever. Ring size: implementation detail, a few dozen.
- **Sharing**: shipped baselines accrete from maintainers' and
  volunteers' local stores by copying the file (it is one
  human-readable TOML; the local-only fields are stripped or ignored on
  import). No mechanism beyond that in v1; an "export calibration"
  affordance belongs on the future Desktop tab's list.
- **Invalidation**: `epoch` is declared in model metadata
  (`metadata.cost.epoch`, default 1) and bumped when an impl's memory
  behaviour changes *without moving any key component* — a new attention
  backend, a preprocessing change, swapped weights under the same
  inference ID. It is per-model (per-ID override) and reaches every user
  through the shipped registry on upgrade. Changes that *do* move a key
  component need no bump: a default-dtype flip (the CLIP FP32→FP16 case)
  re-keys lookups the moment negotiation lands on the new dtype, so all
  old-dtype entries — local and shipped — stop matching automatically;
  torch upgrades likewise via the `torch` key. Stale entries are ignored,
  not deleted.

### Model metadata additions

```toml
[group.clip.metadata.cost]
unit        = "item"
aggregation = "count"
epoch       = 1
seed_units  = 8        # first-touch batch on unknown hardware
```

`seed_units` takes over the *safety* role of `default_batch_size` (the
target role disappears — auto is the target). Per-ID override where an ID
deviates from its group (`dots_ocr`, `easyocr_*`, `qwen3-vl-embedding-*`
all deviate from their groups' dimensions and need per-ID `cost` blocks).

## Batch size UX: auto everywhere, the number becomes a cap

Auto is the only mode; what varies is whether a **cap** is present.
The current single number splits three ways:

1. **User cap** (`Option`, default `None` = no cap) — renamed
   "max batch size" in every UI surface. Request-scoped: core forwards it
   on each inference request; the dispatcher partitions windows by cap
   value and the worker enforces it at pack time as an item-count
   constraint (see the dispatcher section). Inferio stores nothing per
   user/core, so one server serves differently-capped jobs from several
   cores concurrently. Capping only lowers; there is no override above
   the calibrated/knee ceiling.
2. **Core in-flight sizing** — no longer user-facing. Core sizes requests
   by request-level concerns (payload bytes in flight — the existing
   byte-budget pipelining — and keeping the server fed), not by the cap
   and not by guessing GPU batches.
3. **Calibration seed** — inferio-side resolution order: local profile →
   shipped profile → `metadata.cost.seed_units` → global conservative
   constant.

Schema/plumbing (verified): `CronJob.batch_size`, model-config
`default_batch_size`, and job-request `batch_size` are already
`Option<i64>` — `None` = auto with no schema migration.

Migration and surface changes:

- One-time migration nulls **both** the stored last-used defaults
  (`default_batch_size` in per-model system config) **and** cron rows'
  `batch_size` to `None` (= auto). The two are not symmetric — a default
  is only "last selected" and changes nothing until a user manually runs
  a job, while nulling a cron row silently changes what runs unattended
  on the user's machine — but auto is the better setting for the vast
  majority even in cron, re-selecting auto by hand is awkward, and the
  user base is small; the rare intentional cap is re-entered once. Cron
  rows themselves are preserved — only the batch number is cleared.

  Mechanics (both values live in the same per-index-DB `config.toml` —
  `job_settings[].default_batch_size` and `cron_jobs[].batch_size` are
  `SystemConfig` fields — so this is one file rewrite per index DB):

  - **Hook**: a Rust post-migration step for index databases, running
    wherever SQL migrations already run — the startup sweep
    (`migrate_all_databases_on_disk`, which enumerates every index-DB
    directory including ones the user never opens) *and* the per-DB
    open/create path (`migrate_databases_on_disk`). Covering both paths
    is what makes the guard airtight for databases created at runtime
    *after* the upgrade: they get stamped at creation (when nulling a
    default config is a no-op — `migrate_path` already knows `fresh`),
    so a cap the user enters later can never be wiped by a delayed
    first sweep.
  - **Stamp**: a named-row table in the index schema (the
    `maintenance_state` pattern), created empty by a normal sqlx
    migration; the Rust step checks it, and inserts the row only after
    the TOML rewrite succeeds. Config-then-stamp ordering is the
    crash-safe direction: a crash between the two re-runs the null on
    the next startup, which is harmless because no user interaction can
    intervene before that restart completes.
  - **Rewrite**: `SystemConfigStore` load → null the two fields → save,
    which goes through the comment-preserving `TomlDocument`
    patch path and the atomic write. A DB directory with no
    `config.toml` is skipped, not seeded (nothing to null). One
    verification for implementation: `patch_serialized` must *remove*
    a key whose value went `Some → None` — TOML has no null.
- Cron model config must accept and persist "auto" (`None`) — it already
  can; the UI must offer it.
- The Desktop "new database" wizard drops its batch-size control entirely.
- Scan page: "Auto" default, cap as an advanced field whose tooltip fits
  one sentence ("never more than N items at once").

## Rollout order

1. Cost-dimension metadata + the ledger/grant loop: universal
   worker→GPU pinning at spawn, grants and fit snapshots on request
   frames, measurements and memory samples on response frames, tiered
   base on the load response, load reservations (the
   expected-base-vs-headroom *check* rides along; the eviction response
   waits for item 8), packing + defensive clamp in the worker,
   dispatcher cap-rule removal, local store round-trip through the
   orchestrator. easyOCR re-batching (bucketed `max-times-count`)
   **under realistic core pipelining** is the acceptance test.
2. Budget config (margin + cap, per-UUID overrides) and the reactive
   shrink path with `empty_cache()` hysteresis, plus the idle-resident
   trim message.
3. Core/UI: auto/cap rename, wizard removal, the stamped one-time
   migration (last-used defaults + cron rows → auto), cron "auto" in the
   UI.
4. Throughput-knee capture — fitted in units/sec (heterogeneous batches
   make items/sec noisy for `sum` models) with a minimum-sample gate
   before the knee may cap grants; shipped-baseline directory wiring
   (format exists from step 1; shipping actual baselines can lag).
5. Impl-time verifications flagged in the taxonomy table (moondream
   bounds, doctr recognition variance, qwen3 pixel cap, CLAP window).

Item 8 of the parent doc (evict residents pre-load using recorded `base`)
falls out of step 1's footprint data plus the existing generation-guarded
unload machinery; its *trigger* (a load reservation exceeding headroom)
already ships in step 1 — the eviction response slots in after step 2. Item 9 (self-test) pre-warms
profiles using the same harness on synthetic inputs — after step 1 it is a
script, not a subsystem.

## Open questions

- Desktop margin default: 0.10 pending real-world feel; revisit after
  dogfooding on the two-5090 host (asymmetric monitor load is the test).
- Tuning constants bundled as "implementation detail, tune empirically":
  `empty_cache()` hysteresis (deflate ratio, consecutive-window count),
  the clean-window count N that restores a deflated grant, the window
  depth multiplier (2–4×), the widened-margin factor for
  non-local profiles, the `residual_mb` margin-widening clamp, the
  squeeze threshold that triggers an idle-resident trim, the
  throughput-collapse ratio behind the WDDM synthetic negative sample,
  the non-local-profile confirmation sample count, and the
  extrapolation-ratchet factor (default 2×).
- Placement policy on multi-GPU hosts: v1 pins every worker but keeps
  today's placement (first usable GPU, or the registry's explicit
  `devices` pins). Headroom-based placement — put the next load on the
  card whose ledger has the most room — is the natural follow-up once
  ledgers exist.
- ROCm: `mem_get_info`/NVML equivalents exist (HIP, rocm-smi/amdsmi) but
  are untested here by design; the design is backend-agnostic on paper,
  cuda first in practice.
- Whisper stays out of v1; if CT2 footprint recording is ever wanted it
  needs an NVML-based path (no torch allocator) and is Linux-reliable
  only.
