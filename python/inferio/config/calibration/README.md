# Shipped calibration baselines

This directory holds **shipped VRAM cost profiles** for the batch-calibration
system (`docs/batch-calibration-design.md`, "Calibration store"). It is read
by the Rust orchestrator (`panoptikon/src/inferio/calibration.rs`), never by
Python — it lives here because a baseline is the same kind of thing as the
model registry beside it: shipped, read-only, mtime-reloaded knowledge about
models. The registry loader only globs `*.toml` directly inside
`python/inferio/config/`, so this subdirectory is invisible to it.

Like the rest of `python/inferio/config/`, this is **not a user-owned
surface**: it ships with the binary and is replaced on upgrade. Locally
generated calibration lives in `<data_folder>/inferio/calibration.toml` and
overlays whatever is here.

How maintainers produce what goes here — measure on Linux/CUDA, generate the
Windows rows — is `docs/model-cost-measurement.md`, "Producing a shipped
baseline"; the tool is `tools/calibration-protocol/baselines.py`.

## What a profile is

Calibration learns a per-model memory cost model

```
memory ≈ base + slope × units
```

where *unit* is the model's declared cost dimension (`metadata.cost` in the
registry). A profile is one such fit for one model on one **GPU
architecture** in one software environment. It is a property of the kernels
that run and the software around them, not of a particular card or SKU, so a
5070 and a 5090 share one profile and differ only in budget.

A profile is a **prior, never ground truth**: the driver version is
deliberately not part of the key and `base` is driver-currency, so any profile
this machine did not generate is used with a widened safety margin until a few
local measurements confirm it, and it never grants growth beyond what this
machine has measured itself.

## File format

Any number of `*.toml` files, read in file-name order; later files win on an
identical key, as does a later baseline *directory* (a user registry dir's
`calibration/` subdirectory overrides the built-in one). `schema = 3`; a file
whose stamp is not exactly that — newer, older, or absent — is ignored whole.
Schema 3 (2026-09) re-keyed the GPU half from the SKU name to the
architecture, and schema 2 before it re-denominated `slope_mb_per_unit` from
allocator-pool growth to **allocated** memory, so neither older schema is
convertible. A single malformed `[[profile]]` costs only itself — it is
skipped with a warning naming its position in the file, and the rest of the
file still loads. Every `*_mb` quantity is **MiB** (1024², what `nvidia-smi
--format=nounits` and torch's memory statistics both speak).

```toml
schema = 3

[[profile]]
inference_id = "clip/ViT-H-14-378-quickgelu_dfn5b"
epoch        = 1                       # from metadata.cost.epoch; stale-epoch entries are ignored
arch         = "sm_120"                # GPU ARCHITECTURE — the key (see below)
gpu          = "NVIDIA GeForce RTX 5090"   # the SKU this was FIRST measured on.
                                       # Provenance: ignored by matching, and
                                       # kept from the first card when another
                                       # of the same architecture re-measures
platform     = "windows"               # windows | linux | macos
backend      = "cuda"                  # accelerator extra: cuda | rocm | mps | cpu
torch        = "2.7.1+cu128"           # full torch.__version__
dtype        = "fp16"                  # load precision actually in use; "unstated"
                                       # when the impl negotiates none and its
                                       # weights could not be read (a key value,
                                       # not an omission — see the protocol doc).
                                       # Spelled "unknown" before run2 (R11);
                                       # rows under the old spelling no longer
                                       # match and are re-measured
dtype_method = "inferred"              # selected | attribute | inferred | unstated:
                                       # how that precision was arrived at.
                                       # Diagnostic only — the key is `dtype`
unit         = "item"                  # denormalized from metadata, for readability
aggregation  = "count"

base_mb           = 4321               # load footprint, process-level
base_method       = "nvml"             # nvml | fdinfo | mps | rss | free_delta |
                                       # alloc_delta_measured | alloc_delta.
                                       # The two alloc_delta spellings are two
                                       # formulas: the measured one charges the
                                       # context this process measured across
                                       # its first CUDA init (run2 R8), the
                                       # other the fixed 500 MiB estimate
base_platform     = "linux"            # optional: the platform base_mb was
                                       # measured on, when that is not this
                                       # row's own `platform`. Present only on
                                       # a generated cross-platform copy;
                                       # ignored by matching, and reported on
                                       # `GET /api/inference/metadata` so a
                                       # reader of base_mb knows the figure was
                                       # measured elsewhere (see below)
slope_mb_per_unit = 0.79               # marginal cost per unit, MiB of
                                       # *allocated* memory (schema 2)
knee_units        = 512                # optional: the throughput knee. A cap, not
                                       # a ceiling — the orchestrator widens it by
                                       # one log2 bucket after clean windows run
                                       # at it with memory to spare, and withdraws
                                       # it once it can no longer bind (run2, R1d).
                                       # A knee the importing machine did not
                                       # measure itself is *provisional*: it gets
                                       # 4 such windows per step rather than 12,
                                       # so a wrong one is climbed out of in
                                       # seconds rather than never (run2, R1e)
samples           = 38
residual_mb       = 96                 # fit scatter → confidence
measured_at       = "2026-07-30T00:00:00Z"
generator         = "panoptikon 0.1.7"
```

Lookup key: `(inference_id, epoch, arch, unit, aggregation, platform,
backend)`, plus `torch` and `dtype`. The torch string falls back one tier: an
exact match wins, otherwise the same `major.minor` matches (the local version
tag and the patch level are ignored; `backend` already encodes the CUDA/ROCm
family). `epoch` is the deliberate invalidation lever — bumped in the model's
registry metadata when an impl's memory behaviour changes without moving any
other key component. Stale entries are ignored, never deleted.

## The architecture key

`arch` is the GPU half of the key because memory per unit follows which
kernels run, and kernel choice follows compute capability rather than the SKU.
Four spellings, one per host family, and the host and the loaded worker derive
them the same way by construction:

| `arch` | Where it comes from |
|---|---|
| `sm_120` | CUDA: `sm_<major><minor>` from the compute capability — `nvidia-smi --query-gpu=compute_cap` on the host, `torch.cuda.get_device_capability` in the worker |
| `gfx1100` | ROCm: the ISA name — decoded from KFD's packed `gfx_target_version` on the host, `gcnArchName` with everything after `:` stripped in the worker |
| `apple-m3` | MPS: the chip family from the CPU brand string (`Apple M3 Max` → `apple-m3`) |
| `cpu` | A RAM-priced host |

On CUDA and ROCm the host can name the architecture from facts the inventory
already holds, so a stored profile prices even the first load of a run. On MPS
and CPU only a loaded worker can answer, so those hosts learn it from the
first load report on the card, and the load before it is priced from the
conservative constant.

`gpu` is the SKU name, kept as **provenance** and ignored by matching. It is
the display name too, so `GET /api/inference/health` prints it. On ROCm it is
not read off any tool: the orchestrator derives it from kernel sysfs facts —
the GPU's KFD `gfx_target_version` and its VRAM total rounded to the nearest
GiB, giving `AMD gfx1100 (24 GB)` — so it is byte-identical on every host
carrying that silicon, where an amd-smi marketing name would have been
environment-dependent. A ROCm row otherwise differs only in the obvious
places: `platform = "linux"` (the rocm extra is Linux-only), `backend =
"rocm"`, `torch = "2.11.0+rocm7.2"`. `backend` keeps the two families apart on
its own: a cuda-keyed profile never answers a rocm lookup, whatever else
matches.

Nothing ROCm-keyed ships yet: none of it could be measured. The first
baselines there will come from volunteers' local stores, as below.

## The platform rule

`platform` stays in the key: the base figure is platform-flavoured — the
Windows pass read wd-vit's base as `free_delta` 845 MiB where Linux read 964
from NVML — and Windows takes different code paths in places, whisper's cuDNN
DLL branch, CTranslate2's device choice, flash-attention builds that are not
shipped there.

What does travel is the slope, wherever the kernels are the same: the same
pass fitted wd-vit at **29.8594** MiB/item against Linux's **29.8587**. So
shipped Windows rows are **generated from the Linux measurement**, not
measured on Windows, for the ids the registry allowlists with
`metadata.cost.platform_copies`. A generated row carries the Linux `base_mb`
and states `base_platform = "linux"`, which is how a reader of `base_mb` —
"can this GPU load this model?" — knows the figure was measured elsewhere. An
id whose kernels differ per platform is never allowlisted. The process, the
tool and the current allowlist: `docs/model-cost-measurement.md`, "Producing a
shipped baseline".

## Contributing a baseline

Baselines accrete from maintainers' and volunteers' local stores. To
contribute one, copy entries out of your
`<data_folder>/inferio/calibration.toml` into a file here.

The local store carries four fields of *local evidence* — `local_samples`,
`sample_units`, `sample_delta_mb`, `knee_clean_windows`: how much local
evidence stands behind the fit, the raw samples it was fitted from, and (run2,
R1d) how many clean windows that machine has already run at `knee_units`
towards retiring it. They are **stripped on import**, so you may leave them in
the copied file; they will be ignored. Nothing else needs editing.

`max_units_measured` and `knee_units` are *not* stripped. The anchor travels:
any matching profile — shipped or local — floors the ramp at the largest batch
it recorded and caps growth at `RATCHET_FACTOR ×` that figure, so a fresh host
reaches a working size in a few grants instead of a dozen. The card name is not
a gate (a 12 GB and a 32 GB card of one architecture share the row; the
importer's own headroom bounds every grant). The backstop is the out-of-memory
window: it halves an anchor this host has not measured itself, where a locally
measured one stands (run2 B4/N5), and a seeded anchor is never written back to
the local store as this machine's own. A knee can only ever make a grant
smaller, which is the other authority a foreign profile has beyond pricing.
What does not travel with either is the progress towards re-testing the knee:
those windows ran on your GPU, not on the importer's.

There is one cap the store deliberately cannot express, and it is worth
knowing about when a model's `/health` reports a `unit_budget` far below its
`max_units_measured`: the **shape ceiling** (run2 S1). Some impls have a hard,
size-dependent kernel limit that is not a memory condition at all — easyOCR's
detector hits a 32-bit index limit in CRAFT's first pooling kernel at 28 items
of a 1824×2560 padded tensor, whatever the GPU has free — and the worker
reports the trim as `clamped.reason = "index_limit"`. The orchestrator caps the
budget and the ramp at the reported size and shows it as `shape_ceiling_units`.

It is **runtime-only** and appears in no profile, here or in a local store, and
that is deliberate rather than an omission: it depends on *your corpus's*
padded dimensions and on the pixel canvas the clamped window was priced under,
so the same physical kernel limit is a different number for a library of A4
scans and one of thumbnails, and a different number again after a canvas or
`epoch` change. A restart re-learns it from the first clamped window, which
costs one window. Contributing one would hand every importer a cap measured
against a corpus they will never see. Do not add the field, and do not
hand-write one.

A knee you contribute will be treated as **provisional** on every machine that
imports it (run2, R1e): it caps from the first grant, and it is re-tested after
`KNEE_SEED_REVALIDATION_WINDOWS` = 4 clean windows run at it rather than the 12
a locally measured knee gets, widening one log2 bucket at a time until either
the importer's own observations re-fit it or it stops binding and is withdrawn.
So a knee that is right for your GPU costs its importers a probing window
every five; a knee that is wrong for theirs costs them seconds. Contribute the
one you measured, and do not hand-tune it downward "to be safe" — a knee too
low is the failure mode that used to be permanent.

Two things make a baseline worth shipping: it was measured under real load
(not a single window), and `residual_mb` is small relative to `base_mb` — a
scattered fit widens every consumer's margin, which is safe but slow.
