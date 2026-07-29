# Batch calibration and VRAM budgets — design

Package 2 of the GPU compatibility work: the concrete design for items 5–8
of `gpu-compatibility-design.md` (self-calibrating batch size, pixel-budget
admission, footprint recording, VRAM-aware behaviour). Decided 2026-07-30;
supersedes the one-line itemization in that document. Not yet implemented.

## Core decision: learn a cost model, not a max batch size

Calibration does **not** learn "the batch size that fits". It learns a
per-model **memory cost model**

```
memory ≈ base + slope × units
```

where `base` is the load footprint (weights + fixed overhead), `slope` is
the marginal cost per unit of input, and *unit* is a model-specific cost
dimension declared in the model's metadata. Every batch is then sized at
**admission time**: read the currently available budget, subtract `base`,
divide by `slope`, pack inputs up to that many units.

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
  is fitted from *measurements of our own usage at safe sizes*
  (`max_memory_allocated()` deltas) and extrapolated — the OOM boundary is
  predicted, never sought. The Package-1 OOM halving loop remains as a
  backstop for prediction error and external-usage races, not as the
  mechanism.

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

- `max-times-count` models benefit most from **bucketing**: admission sorts
  the pending queue by per-item units and packs batches from
  similarly-sized neighbours, so one 8000×6000 scan doesn't tax 63
  thumbnails. This is what finally retires easyOCR's
  `enable_batching = false`.
- For `pixel` units, "units" means decoded pixels *as submitted* (after
  input-spec slicing/downscale) — the same quantity
  `slice_settings.mode = "pixels"` already reasons about upstream.

## Where each piece runs

The inference server (inferio) is independent of core, can be remote, and
can serve several cores; there can be several inferio servers, each with
several GPUs. Therefore:

- **Admission and measurement run in the Python worker** — it is the only
  place that has torch, the allocator statistics, and `mem_get_info`, and
  the only place that can act between batches. A harness around `predict()`
  (sibling to `run_with_oom_retry`) does budget→units→pack, measures, and
  ramps. Workers receive their profile snapshot + budget config at spawn
  and report measurements back over the existing worker protocol
  (a new message type beside the error frames).
- **The Rust orchestrator owns persistence** — workers are transient. It
  merges measurement reports into the local calibration store (atomic TOML
  rewrite), loads shipped baselines, serves the merged view to workers at
  spawn, and exposes state read-only over the API for UI/labels
  (`/api/inference/metadata` overlay, like `unavailable` today).
- **Core keeps its opaque-ID worldview** — it learns nothing about VRAM,
  GPUs, or profiles. It forwards the user cap per request and sizes its
  *requests* by its own concerns (payload memory in flight, pipelining),
  not by guessing GPU batches. The orchestrator/worker re-slices whatever
  arrives.

Two keyspaces, deliberately different:

- **Cost profiles** are keyed by GPU *model* (`name` string) + environment
  tuple — a property of the silicon and software, shareable.
- **Budgets and budget settings** are keyed by GPU *instance* (board UUID,
  `GPU-…` from NVML/nvidia-smi/torch device properties). Two identical
  cards on one host share profiles but can carry different budget settings
  (e.g. the one driving the monitors gets a bigger margin). CUDA device
  index is **never** an identity — it is not stable across reboots or
  `CUDA_VISIBLE_DEVICES` changes.

## Admission algorithm (per batch, worker-side)

```
free, total   = mem_get_info(device)          # device-wide, other processes included
ours          = memory_reserved(device)        # allocator pool = our footprint as the driver sees it
other         = total − free − ours

limit_total   = total × cap_fraction           # if configured (server lever)
limit_margin  = total − other × (1 + margin)   # if configured (desktop lever, default on)
budget        = min(configured limits) − base_if_not_resident

max_units     = clamp((budget − predicted_current_batch_overhead) / slope,
                      0, throughput_knee_units, request_cap_units)
```

- Pack inputs (bucketed for `max-times-count`) up to `max_units`; a batch
  is never smaller than one item — a single item over budget goes through
  anyway (the backstop catches it if it truly cannot run; Package 1 already
  decided batch-1 OOM = item fails, job continues).
- **Ramp**: until the profile has enough samples, do not jump to the
  predicted ceiling — geometric ramp (seed, ×2 per clean batch) toward it,
  measuring each step. A too-low seed costs a logarithmic number of
  batches, which is why seeds don't need per-GPU tuning.
- **Measurement**: `max_memory_allocated()` delta around `predict()` +
  reset per batch, paired with batch units. Load footprint (`base`)
  measured once around model load. Fit is a robust two-parameter fit;
  retain scatter (sample count, residual) as confidence.
- **Reactive shrink**: budget is recomputed every batch, so rising external
  usage shrinks the next batch. Freeing our tensors is not enough to give
  memory *back* — the allocator pool holds it — so when the budget target
  falls materially below `memory_reserved()` (hysteresis: e.g. below 80%
  of pool for 2 consecutive batches), call `empty_cache()` between batches.
  Hysteresis exact thresholds: implementation detail, tune empirically.
- **Backstop**: `run_with_oom_retry` unchanged. An OOM despite admission
  is recorded as a negative sample (prediction was wrong or the world
  moved) and temporarily deflates the effective budget for that worker.

The only timing assumption: external usage doesn't swing by more than the
margin between two consecutive batches. The backstop covers the exceptions.

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

base_mb           = 4321               # load footprint
slope_kb_per_unit = 812.5              # marginal cost
knee_units        = 512                # optional: throughput stopped improving here
samples           = 38
residual_mb       = 96                 # fit scatter → confidence / safety margin
measured_at       = "2026-07-30T00:00:00Z"
generator         = "panoptikon 0.1.8" # provenance
```

Key tuple for lookup: `(inference_id, epoch, gpu, platform, backend,
torch, dtype)`.

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
- **Invalidation**: `epoch` is declared in model metadata
  (`metadata.cost.epoch`, default 1) and bumped when an impl's memory
  behaviour changes (dtype policy, attention backend, torch generation
  bump also changes the `torch` key naturally). Stale entries are ignored,
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
   on each inference request; the worker applies
   `min(cap, admission result)`. Inferio stores nothing per user/core, so
   one server serves differently-capped jobs from several cores
   concurrently. Capping only lowers; there is no override above the
   calibrated/knee ceiling.
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

- One-time reset of the stored last-used defaults (`default_batch_size` in
  per-model system config) to `None`. Cron rows are **not** reset: their
  numbers survive as caps, which is honest — Package 1's halving already
  made them ceilings in practice.
- Cron model config must accept and persist "auto" (`None`) — it already
  can; the UI must offer it.
- The Desktop "new database" wizard drops its batch-size control entirely.
- Scan page: "Auto" default, cap as an advanced field whose tooltip fits
  one sentence ("never more than N items at once").

## Rollout order

1. Cost-dimension metadata + admission/measurement harness in the worker,
   with the local store round-trip through the orchestrator. easyOCR
   re-batching (bucketed `max-times-count`) is the acceptance test.
2. Budget config (margin + cap, per-UUID overrides) and the reactive
   shrink path with `empty_cache()` hysteresis.
3. Core/UI: auto/cap rename, wizard removal, last-used reset migration,
   cron "auto".
4. Throughput-knee capture; shipped-baseline directory wiring (format
   exists from step 1; shipping actual baselines can lag).
5. Impl-time verifications flagged in the taxonomy table (moondream
   bounds, doctr recognition variance, qwen3 pixel cap, CLAP window).

Item 8 of the parent doc (evict residents pre-load using recorded `base`)
falls out of step 1's footprint data plus the existing generation-guarded
unload machinery; it slots in after step 2. Item 9 (self-test) pre-warms
profiles using the same harness on synthetic inputs — after step 1 it is a
script, not a subsystem.

## Open questions

- Desktop margin default: 0.10 pending real-world feel; revisit after
  dogfooding on the two-5090 host (asymmetric monitor load is the test).
- `empty_cache()` hysteresis thresholds (deflate ratio, consecutive-batch
  count).
- Whether `residual_mb` should widen the effective margin automatically
  (high-scatter models get more headroom) or just gate "verified" labels
  in the future Desktop tab.
- ROCm: `mem_get_info`/NVML equivalents exist (HIP, rocm-smi) but are
  untested here by design; the design is backend-agnostic on paper, cuda
  first in practice.
- Whisper stays out of v1; if CT2 footprint recording is ever wanted it
  needs an NVML-based path (no torch allocator) and is Linux-reliable
  only.
