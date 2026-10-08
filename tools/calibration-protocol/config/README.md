# Server configs for the calibration tools

A configuration is named by id (`legs.py --config C1`, `run-gateway.sh C1`)
and generated on demand: the tree's shipped `config/server/default.toml` with
a few lines changed, plus the environment the gateway is started with. The
table is `CONFIGS` in `../legs.py`; `legs.py --config <id> --write-config DIR`
writes both files out. The shipped configs are never edited (CLAUDE.md: the
server TOMLs are seeded once and user-owned).

**Paths follow `--repo`**, which defaults to the checkout `legs.py` lives in:
the binary, `python/.venv`, the inference sources and `.env` all come from
it, and C0's tree is `panoptikon-master` beside it. From a worktree with no
venv of its own, pass `--repo /path/to/main/checkout` (or `--python`);
`run-gateway.sh` takes the same as `CALIB_REPO=<checkout>` (or
`CALIB_WORKER_PYTHON=<python>`), and `CALIB_PORT` / `CALIB_INFERENCE_URL` as
`--port` / `--inference-url`. A missing
`config/server/default.toml` or venv stops the leg with a message naming it.
A caller's `PANOPTIKON_BIN` picks the binary for every id except C0, which
always runs its own tree's build.

| id | configuration | ports (main/test/legacy, ui) | tree |
|---|---|---|---|
| `C1` | the branch under test, both GPUs visible | 6342 / 6343 / 6339, 6340 | this checkout |
| `C0` | "before" baseline | 6352 / 6353 / 6349, 6350 | `../panoptikon-master` beside it |
| `C2` | C1 + `CUDA_VISIBLE_DEVICES=GPU-<uuid>` (UUID form) | 6362 / 6363 / 6359, 6360 | this checkout |
| `C3` | C1 + `CUDA_VISIBLE_DEVICES=1` (index form) | 6372 / 6373 / 6369, 6370 | this checkout |
| `C7` | C1 + the user registry `registry-C7/registry-C7.toml` (MobileCLIP-S1 pinned to GPU 1; `enable_batching = true` on `doctr/easyocr_standard_en`) | 6382 / 6383 / 6379, 6380 | this checkout |
| `C7nc` | C7 with its registry's `metadata.cost.canvas_pixels` removed **and** `config.canvas_size = 40000` — the control that separates the per-item pixel cap from the `enable_batching` flag (diagnostic, not a proposed configuration; see "Running an uncapped control" below for why both halves are needed) | 6392 / 6393 / 6389, 6390 | this checkout |
| `R1` | C1 on ROCm: `accelerator = "rocm"` in `[inference_local.python_env]`, no cuDNN `LD_LIBRARY_PATH`, `RUST_LOG` adds `panoptikon::db::batch_auto=debug` | 6402 / 6403 / 6399, 6400 | this checkout |
| `R2` | R7 + `HIP_VISIBLE_DEVICES=1`: an ambient HIP-layer restriction, so the inventory stays unknown (unpriced) and the pin is dropped | 6412 / 6413 / 6409, 6410 | this checkout |
| `R3` | R1 + `ROCR_VISIBLE_DEVICES=1` + `registry-R3/registry-R3.toml` (MobileCLIP-S1 pinned to HIP device 0, the only one left): the same at the ROCr layer, which keeps the pin | 6422 / 6423 / 6419, 6420 | this checkout |
| `R7` | R1 + `registry-R7/registry-R7.toml` (MobileCLIP-S1 pinned to HIP device 1) | 6432 / 6433 / 6429, 6430 | this checkout |

C4–C6 are not here: they are Docker configurations (image build args and
compose overlays, `../compose/`).

A ROCm configuration (`R*`) refuses to start when `HIP_VISIBLE_DEVICES`,
`ROCR_VISIBLE_DEVICES`, `CUDA_VISIBLE_DEVICES` or `GPU_DEVICE_ORDINAL` is
inherited and the configuration does not set it itself: the gateway would
leave every GPU unpriced.

`registry-C7/`, `registry-C7nc/`, `registry-R3/` and `registry-R7/` are
directories of their own because `[inference_local].config_dirs` scans
**every** `*.toml` in each directory it is given. The registry file sets `allow_override = true` and restates each
redefined id in full — redefinition replaces the id's config *and* its
id-level metadata, so an omitted `metadata.cost` would silently fall back to
the group's default.

## Running an uncapped control

Removing `metadata.cost.canvas_pixels` from a registry **no longer makes a
model uncapped**, and a control that only does that measures the capped
configuration under a different name.

The canvas has two sources, and the registry is only the first of them
(`docs/inferio-worker-protocol.md`, "Memory grants"):

1. `metadata.cost.canvas_pixels` in the registry;
2. **what the loaded impl states about itself**, which the worker reads at
   load and reports back on the `load ok` response — so the orchestrator caps
   by it too, exactly as if the registry had declared it.

`inferio.impl.eocr` states one (`canvas_pixels = canvas_size ** 2`, default
2560² = 6 553 600) because it now enforces one: it resizes every input onto
that canvas before it pads a batch. Delete the registry key and tier 2 fills
the hole with the same number.

To genuinely remove the cap, raise the impl's own canvas above every image in
the corpus, in the same registry entry:

```toml
[group.doctr.inference_ids.easyocr_standard_en]
config.impl_class      = "easyocr"
config.enable_batching = true
config.canvas_size     = 40000          # 1 600 000 000 px: caps nothing
metadata.cost.epoch    = 2
# metadata.cost.canvas_pixels           # deliberately absent
```

`registry-C7nc/registry-C7nc.toml` ships exactly this. Three things to know
about the figure:

* it must exceed the **longest side** of every input (the corpus's largest is
  8 000 px), not the area — the impl's ceiling is a side length and the pixel
  figure is its square;
* keep `canvas_size ** 2` inside `u32` (`panoptikon/src/inferio/cost.rs:154`
  types the wire field `Option<u32>`); 40 000 gives 1.6 × 10⁹, and anything
  above 65 535 does not fit;
* it changes **pricing and packing only**. `canvas_size` reaches easyOCR's own
  `Reader.detect` as a per-request parameter, never from this config, so the
  CRAFT detector still resizes onto its own 2560 px canvas and the control is
  not a different model — which is the point: it isolates the pixel cap.

Confirm from the log before trusting a leg: the `load ok` line should carry
`canvas_pixels=1600000000` and the window's `sample_units` should hold raw
pixel counts (no multiple of 6 553 600).

## Running one

```
./run-gateway.sh C1 /abs/path/to/results/<run-id>/<scenario>
```

The second argument becomes `--root`, which panoptikon implements as a chdir
at startup, so the scenario owns its own `data/panoptikon.log`,
`data/inferio/calibration.toml`, `data/index/*` and `data/tmp`. That chdir is
also why each config pins `[inference_local]`'s `python`, `impl_dirs`,
`config_dirs` and `pythonpath` to absolute paths, and why `run-gateway.sh`
exports the checkout's `.env` itself instead of relying on the CWD auto-load.

The deviations from the shipped default are the ports, `[upstreams.ui] local
= false` and the absolute inference paths (`legs.py`, `render_config`).
Everything else — including the empty `[inference_local.vram]` table, so
`margin` stays at its built-in default and `cap_fraction` stays off — is the
shipped file.

Setting `python` also short-circuits the startup auto-setup
(`setup.rs::maybe_auto_setup` returns early when `python` is set). That is
deliberate: the venvs are synced by hand, the branch one **with** the `test`
group, and the server's own `uv sync --locked --extra cu128` would uninstall
it.

## Adding the fault-injection fixtures

See `../fixtures/README.md`. Either run `../fixtures/install-fixtures.sh`
(copies into `inferio_custom/` and `config/inference/`, which these configs
already point at) or extend `impl_dirs` / `config_dirs` here.
