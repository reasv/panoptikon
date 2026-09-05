"""Fixture impl that OOMs on its *second* GPU batch, on a real CUDA GPU.

CUDA-touching variant of the torch-free fixture of the same name in
`python/tests/inferio_worker/fixture_impls/`: it holds one CUDA tensor for the
model's lifetime, which is what makes the worker report a `gpu_uuid` and a
`base_mb` and the ledger price it. Unlike the original it bounds the OOMs, so
one negative settle is what the scenario actually sees.

Config keys (registry TOML, passed as **kwargs):
  load_mb: MiB held for the model's lifetime (default 64).
  device:  torch device string (default "cuda"); the worker is pinned with
           CUDA_VISIBLE_DEVICES, so "cuda" is always the intended GPU.
  oom_batches: how many batches OOM, starting with the second (default 1).
           0 disables the OOM; a large value reproduces the torch-free
           behaviour.

See tools/calibration-protocol/fixtures/README.md "Why a CUDA-touching
variant exists".
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
