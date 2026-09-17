# Server configs for the calibration protocol

One copy of the shipped `config/server/default.toml` per configuration id from
`docs/batch-calibration-test-protocol.md` §3, plus the environment each one
needs and a launcher. The shipped configs are never edited (CLAUDE.md: the
server TOMLs are seeded once and user-owned).

| file | configuration | ports (main/test/legacy, ui) | binary |
|---|---|---|---|
| `server-C1.toml` + `env.C1` | primary: PR branch, both GPUs visible | 6342 / 6343 / 6339, 6340 | branch `target/release/panoptikon` |
| `server-C0.toml` + `env.C0` | "before" baseline: master worktree | 6352 / 6353 / 6349, 6350 | `../../../panoptikon-master/target/release/panoptikon` |
| `server-C2.toml` + `env.C2` | C1 + `CUDA_VISIBLE_DEVICES=GPU-<uuid>` (UUID form) | 6362 / 6363 / 6359, 6360 | branch |
| `server-C3.toml` + `env.C3` | C1 + `CUDA_VISIBLE_DEVICES=1` (index form) | 6372 / 6373 / 6369, 6370 | branch |
| `server-C7.toml` + `env.C7` | C1 + the user registry `registry-C7/registry-C7.toml` (MobileCLIP-S1 pinned to GPU 1; `enable_batching = true` on `doctr/easyocr_standard_en`) | 6382 / 6383 / 6379, 6380 | branch |
| `server-C7nc.toml` + `env.C7nc` | C7 with its registry's `metadata.cost.canvas_pixels` removed **and** `config.canvas_size = 40000` — the run2 Phase-D1 control that separates R7's per-item pixel cap from the `enable_batching` flag (diagnostic, not a proposed configuration; see "Running an uncapped control" below for why both halves are needed) | 6392 / 6393 / 6389, 6390 | branch |

C4–C6 are not here: they are Docker configurations (image build args and
compose overlays, Phase 6).

`registry-C7/` and `registry-C7nc/` are directories of their own because `[inference_local].config_dirs`
scans **every** `*.toml` in each directory it is given, and this directory holds
the server configs, which are not registries. The registry file sets
`allow_override = true` and restates each redefined id in full — redefinition
replaces the id's config *and* its id-level metadata, so an omitted
`metadata.cost` would silently fall back to the group's default.

## Running an uncapped control

Removing `metadata.cost.canvas_pixels` from a registry **no longer makes a
model uncapped**, and a control that only does that measures the capped
configuration under a different name.

Since run2's D1-b fix the canvas has two sources, and the registry is only the
first of them (`docs/inferio-worker-protocol.md`, "Memory grants"):

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
  not a different model — which is the point: it isolates R7.

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

The three deviations from the shipped default (ports, `[upstreams.ui] local =
false`, the absolute inference paths) are marked `CALIB` and explained inline
in each file. Everything else — including the empty `[inference_local.vram]`
table, so `margin` stays at its built-in 0.10 and `cap_fraction` stays off —
is byte-identical to `config/server/default.toml`.

Setting `python` also short-circuits the startup auto-setup
(`setup.rs::maybe_auto_setup` returns early when `python` is set). That is
deliberate: the venvs are synced by hand in Phase 0, the branch one **with**
the `test` group, and the server's own `uv sync --locked --extra cu128` would
uninstall it.

## Adding the fault-injection fixtures

See `../fixtures/README.md`. Either run `../fixtures/install-fixtures.sh`
(copies into `inferio_custom/` and `config/inference/`, which these configs
already point at) or extend `impl_dirs` / `config_dirs` here.
