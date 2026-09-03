# Calibration-protocol fixtures

Fault-injection impls and the user registry that exposes them, for
`docs/batch-calibration-test-protocol.md` §3 (models table, `fixture` row) and
the scenarios that need a deterministic OOM, a batch-1 OOM, a non-OOM
merged-batch failure, or a worker death on demand (S5 and friends).

```
fixtures/
  impls/                     CUDA-touching variants (built in Phase 0)
    oom_second_batch_cuda_impl.py   "oom_second_batch_cuda_test"
    oom_cuda_impl.py                "oom_cuda_test"
    failbatch_cuda_impl.py          "failbatch_cuda_test"
    dying_cuda_impl.py              "dying_cuda_test"
    oom_timed_cuda_impl.py          "oom_timed_cuda_test"    (Phase 4)
    dies_on_load_cuda_impl.py       "dies_on_load_cuda_test" (Phase 4)
  registry/
    calibration-fixtures.toml  group `calibfixture`, 8 inference ids
  install-fixtures.sh          copies both into the shipped default locations
```

## Why a CUDA-touching variant exists

The shipped fixtures in `python/tests/inferio_worker/fixture_impls/` are
deliberately torch-free. On a CUDA host that means the worker's load report
carries no `gpu_uuid` and no `base_mb`: `_finish_load`'s `touched_gpu` gate
(`python/inferio_worker/memory.py`) stays shut because neither the allocated
nor the reserved counter moved, and `device_identity()` has no live CUDA
context to read. `VramLedger::resolve_board` then has nothing to join on, so
on a two-board host the fixture is never admitted to a ledger and its windows
run **unpriced** — which is the opposite of what the fixture scenarios are
meant to exercise. (With exactly one visible board the single-board fallback
still admits it, which is why the torch-free ids remain usable on C2.)

Each variant therefore allocates and touches one `float32` tensor of
`load_mb` MiB (default 64) on the pinned device inside `load()`, and holds it
for the model's lifetime. That initialises CUDA and moves the allocator
counters, which is all the two gates need.

Measured on this host (Phase 0, direct `begin_load` / `load()` /
`finish_load` in the venv, GPU 1, no gateway):

| field | value |
|---|---|
| `base_mb` | 722 |
| `base_method` | `nvml` |
| `reserved_at_load_mb` | 64 |
| `gpu_uuid` | `GPU-01c61d5b-6b4c-bd6a-019b-150586096a47` |
| `gpu_name` | `NVIDIA RTX PRO 6000 Blackwell Workstation Edition` |
| `gpu_total_mb` | 97250 (torch) vs 97887 NVML board total — 0.7 %, inside the ±5 % sample check |
| `memory.free_source` | `nvml` |

722 MB is the CUDA context (~658 MB on this driver) plus the 64 MB ballast, so
the "model" is priced as a ~700 MB resident with a zero slope. Raise `load_mb`
in the registry if a scenario wants a heavier fixture.

The variants are self-contained (stdlib + torch only): the worker's
`discovery.py` loads each `*.py` as a standalone module by file location, so
relative imports between fixture files do not work.

## Installing

Two equivalent routes; the second leaves the checkout clean.

1. `./install-fixtures.sh` copies the four torch-free originals and the four
   CUDA variants into `<tree>/inferio_custom/` and the registry TOML into
   `<tree>/config/inference/` — the two directories the shipped defaults
   scan (`resources.rs::default_impl_dirs`, `registry.rs`'s default
   `config_dirs`). `--uninstall` removes them again. Note these defaults are
   resolved against the process CWD, and `--root` chdirs, so they only work
   because `../config/server-C*.toml` pins the absolute paths.

2. Add the two directories to the config instead:

   ```toml
   [inference_local]
   impl_dirs = [
     "/home/admin/projects/panoptikon/python/inferio/impl",
     "/home/admin/projects/panoptikon/inferio_custom",
     "/home/admin/projects/panoptikon/python/tests/inferio_worker/fixture_impls",
     "/home/admin/projects/panoptikon/tools/calibration-protocol/fixtures/impls",
   ]
   config_dirs = [
     "/home/admin/projects/panoptikon/python/inferio/config",
     "/home/admin/projects/panoptikon/config/inference",
     "/home/admin/projects/panoptikon/tools/calibration-protocol/fixtures/registry",
   ]
   ```

   Built-in dirs must stay first: the first module providing a matching
   `name()` wins and nothing may shadow a shipped impl class.

## Driving them

The fixtures return `{"batch": n}` / `{"ok": true}`, not tags, so drive them
through `POST /api/inference/predict/calibfixture/<id>` (that is what
`loadgen.py` does), not through an extraction job — the job would reject the
payload against the declared `output_type`.

Inference ids: `calibfixture/{oom_second_batch,oom,failbatch,dying}_{cuda,cpu}`,
plus the two Phase-4 additions `calibfixture/oom_timed_cuda` (batch-1 OOM for
`oom_secs` after load, healthy afterwards — the only way to time deflation's
*recovery* on one resident worker) and `calibfixture/dies_on_load_cuda`
(raises inside `load()`, for the respawn-cadence measurement, finding B15).
Use the `_cuda` ids on C1 (priced, board-resolved) and either family on C2.

`oom_second_batch_cpu` (the shipped torch-free impl) tests `batches >= 2`, so
it OOMs on the second batch **and on every batch after it, for the worker's
whole lifetime**. Under a real gateway the per-request fallback turns that
into one negative settle per retry: Phase 4's first S5 leg reached
`deflation = 2 227` in 40 s. `oom_second_batch_cuda` therefore takes an
`oom_batches` config key (default **1**), so it OOMs exactly once — which is
the case §4 S5 describes. Set `oom_batches` high to get the old behaviour;
use `calibfixture/oom_cuda` for a permanent OOM.
