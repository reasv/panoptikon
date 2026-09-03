"""Fixture impl that OOMs on its *second* GPU batch, on a real CUDA board.

CUDA-touching variant of the torch-free fixture of the same name
(`python/tests/inferio_worker/fixture_impls/`), built for Phase 0 of
`docs/batch-calibration-test-protocol.md` §3 (fixture note): a torch-free
fixture reports no `gpu_uuid`/`base_mb`, so on a multi-board CUDA host the
ledger cannot resolve it to a board and it runs unpriced. This variant
allocates and touches one small CUDA tensor inside `load()`, which
(a) initialises CUDA so `memory.py` can read `get_device_properties(0)` and
report `gpu_uuid`/`gpu_name`/`gpu_total_mb`, and (b) moves the torch
allocator's reserved/allocated counters across the load window so the
`touched_gpu` gate in `_finish_load` opens and a `base_mb` is resolved
(`nvml` own-PID tier on bare Linux).

Deliberate deviation from the torch-free original, found in Phase 4: the
shipped fixture's test is `self.batches >= 2`, so it OOMs on the second batch
**and on every batch after it, forever**. Under a real gateway that is not
"one OOM": the harness falls back to per-request prediction, each retry is
its own batch, each raises, and the ledger books a negative settle for every
one — `deflation` reached 2 227 in 40 s in the first S5 leg. That is the
`oom_cuda` fixture's job. Here the OOM count is bounded by `oom_batches`
(default 1), so the scenario the protocol describes — one negative,
`deflation = 1`, recovery after three clean windows — is what actually
happens.

Config keys (from the registry TOML, passed as **kwargs):
  load_mb: MiB of device memory to hold for the model's lifetime (default 64).
  device:  torch device string (default "cuda"); the worker is pinned with
           CUDA_VISIBLE_DEVICES so "cuda" is always the intended board.
  oom_batches: how many batches OOM, starting with the second (default 1).
           0 disables the OOM; a large value reproduces the shipped
           torch-free behaviour.

Keep this stdlib+torch only and self-contained: the worker's discovery
(`inferio_worker/discovery.py`) loads each file as a standalone module, so
relative imports between fixture files do not work.
"""

import torch


class OomSecondBatchCudaModel:
    def __init__(self, **config):
        self.config = config
        self.load_mb = int(config.get("load_mb", 64))
        self.device = str(config.get("device", "cuda"))
        self.oom_batches = int(config.get("oom_batches", 1))
        self._ballast = None

    def predict(self, inputs):
        self.batches = getattr(self, "batches", 0) + 1
        if 2 <= self.batches < 2 + self.oom_batches:
            raise RuntimeError(
                "CUDA out of memory. Tried to allocate 2.00 GiB (fixture)"
            )
        return [{"batch": len(inputs)} for _ in inputs]

    @classmethod
    def name(cls) -> str:
        return "oom_second_batch_cuda_test"

    def load(self) -> None:
        if not torch.cuda.is_available():
            raise RuntimeError(
                "oom_second_batch_cuda_test requires CUDA: torch.cuda.is_available() is False"
            )
        # float32 -> 4 bytes per element; touch it so the pages are real.
        elems = max(1, (self.load_mb * 1024 * 1024) // 4)
        self._ballast = torch.empty(elems, dtype=torch.float32, device=self.device)
        self._ballast.fill_(1.0)
        torch.cuda.synchronize()

    def unload(self) -> None:
        self._ballast = None
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


IMPL_CLASS = OomSecondBatchCudaModel
