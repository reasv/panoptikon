"""Fixture impl whose predict fails for merged batches only, on a real CUDA GPU.

CUDA-touching variant of the torch-free fixture of the same name
(`python/tests/inferio_worker/fixture_impls/`), built for Phase 0 of
`docs/batch-calibration-test-protocol.md` §3 (fixture note): a torch-free
fixture reports no `gpu_uuid`/`base_mb`, so on a multi-GPU CUDA host the
ledger cannot resolve it to a GPU and it runs unpriced. This variant
allocates and touches one small CUDA tensor inside `load()`, which
(a) initialises CUDA so `memory.py` can read `get_device_properties(0)` and
report `gpu_uuid`/`gpu_name`/`gpu_total_mb`, and (b) moves the torch
allocator's reserved/allocated counters across the load window so the
`touched_gpu` gate in `_finish_load` opens and a `base_mb` is resolved
(`nvml` own-PID tier on bare Linux).

Config keys (from the registry TOML, passed as **kwargs):
  message: the ValueError text, `{n}` = batch size (default "refusing merged
           batch of {n}"). Phase 4 uses a second registration whose message
           merely *contains* the words "out of memory" to probe finding B11
           (`message_reports_oom` matches any line containing them).
  load_mb: MiB of device memory to hold for the model's lifetime (default 64).
  device:  torch device string (default "cuda"); the worker is pinned with
           CUDA_VISIBLE_DEVICES so "cuda" is always the intended GPU.

Keep this stdlib+torch only and self-contained: the worker's discovery
(`inferio_worker/discovery.py`) loads each file as a standalone module, so
relative imports between fixture files do not work.
"""

import torch


class FailBatchCudaModel:
    def __init__(self, **config):
        self.config = config
        self.load_mb = int(config.get("load_mb", 64))
        self.device = str(config.get("device", "cuda"))
        self.message = config.get("message") or "refusing merged batch of {n}"
        self._ballast = None

    def predict(self, inputs):
        import time

        time.sleep(0.3)
        if len(inputs) > 1:
            raise ValueError(self.message.format(n=len(inputs)))
        return [{"ok": True} for _ in inputs]

    @classmethod
    def name(cls) -> str:
        return "failbatch_cuda_test"

    def load(self) -> None:
        if not torch.cuda.is_available():
            raise RuntimeError(
                "failbatch_cuda_test requires CUDA: torch.cuda.is_available() is False"
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


IMPL_CLASS = FailBatchCudaModel
