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

## What a profile is

Calibration learns a per-model memory cost model

```
memory ≈ base + slope × units
```

where *unit* is the model's declared cost dimension (`metadata.cost` in the
registry). A profile is one such fit for one model on one **GPU model** in one
software environment. It is a property of the silicon and the software, not of
a particular card, so a 12 GB and a 6 GB variant of the same board share one
profile and differ only in budget.

A profile is a **prior, never ground truth**: the driver version is
deliberately not part of the key and `base` is driver-currency, so any profile
this machine did not generate is used with a widened safety margin until a few
local measurements confirm it, and it never grants growth beyond what this
machine has measured itself.

## File format

Any number of `*.toml` files, read in file-name order; later files win on an
identical key, as does a later baseline *directory* (a user registry dir's
`calibration/` subdirectory overrides the built-in one). `schema = 1`; a file
declaring a newer schema is ignored whole. A single malformed `[[profile]]`
costs only itself — it is skipped with a warning naming its position in the
file, and the rest of the file still loads. Every `*_mb` quantity is **MiB**
(1024², what `nvidia-smi --format=nounits` and torch's memory statistics both
speak).

```toml
schema = 1

[[profile]]
inference_id = "clip/ViT-H-14-378-quickgelu_dfn5b"
epoch        = 1                       # from metadata.cost.epoch; stale-epoch entries are ignored
gpu          = "NVIDIA GeForce RTX 5090"   # GPU model name, exactly as nvidia-smi prints it
platform     = "windows"               # windows | linux | macos
backend      = "cuda"                  # accelerator extra: cuda | rocm | cpu
torch        = "2.7.1+cu128"           # full torch.__version__
dtype        = "fp16"                  # load precision actually in use; "unknown"
                                       # when the impl negotiates none and its
                                       # weights could not be read (a key value,
                                       # not an omission — see the protocol doc)
unit         = "item"                  # denormalized from metadata, for readability
aggregation  = "count"

base_mb           = 4321               # load footprint, process-level
base_method       = "nvml"             # nvml | fdinfo | free_delta | alloc_delta
slope_mb_per_unit = 0.79               # marginal cost per unit, MiB
knee_units        = 512                # optional; not fitted yet (rollout step 4)
samples           = 38
residual_mb       = 96                 # fit scatter → confidence
measured_at       = "2026-07-30T00:00:00Z"
generator         = "panoptikon 0.1.7"
```

Lookup key: `(inference_id, epoch, gpu, platform, backend, torch, dtype)`.
The torch string falls back one tier: an exact match wins, otherwise the same
`major.minor` matches (the local version tag and the patch level are ignored;
`backend` already encodes the CUDA/ROCm family). `epoch` is the deliberate
invalidation lever — bumped in the model's registry metadata when an impl's
memory behaviour changes without moving any other key component. Stale entries
are ignored, never deleted.

## ROCm baselines

Same file, same fields, but three of them are spelled differently and `gpu` is
a key component, so the spelling is load-bearing:

```toml
gpu      = "AMD gfx1100 (24 GB)"   # never a marketing name
platform = "linux"                 # the rocm extra is Linux-only
backend  = "rocm"
torch    = "2.11.0+rocm7.2"        # full torch.__version__, as always
```

The `gpu` string is not read off any tool. The orchestrator *derives* it from
kernel sysfs facts — the board's KFD `gfx_target_version` and its VRAM total
rounded to the nearest GiB — so it is byte-identical on every host carrying
that silicon and cannot appear, disappear or change spelling with what happens
to be installed. An amd-smi/rocm-smi marketing name would have been
environment-dependent, and a key that flips orphans every profile on the
machine. Write it exactly as the running server names the board (it is the
display name too, so `GET /api/inference/health` prints it); the VRAM figure
is what separates SKUs that share a gfx target and do not price alike — a
16 GB and a 24 GB gfx1100.

`backend` keeps the two families apart on its own: a cuda-keyed profile never
answers a rocm lookup, whatever else matches. The torch tier behaves
identically on a `+rocm` local version tag — `2.11.0` and `2.11.1` share a
`major.minor`, `2.10` and `2.11` do not.

Nothing ROCm-keyed ships yet: none of it could be measured. The first
baselines here will come from volunteers' local stores, as below.

## Contributing a baseline

Baselines accrete from maintainers' and volunteers' local stores. To
contribute one, copy entries out of your
`<data_folder>/inferio/calibration.toml` into a file here.

The local store carries four extra fields — `max_units_measured`,
`local_samples`, `sample_units`, `sample_reserved_mb` — that record *local
authority*: the largest batch that machine actually ran, how much local
evidence stands behind the fit, and the raw samples it was fitted from. They
are **stripped on import**, so you may leave them in the copied file; they
will be ignored. Nothing else needs editing.

Two things make a baseline worth shipping: it was measured under real load
(not a single window), and `residual_mb` is small relative to `base_mb` — a
scattered fit widens every consumer's margin, which is safe but slow.
