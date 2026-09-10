"""Fixture impl whose predict fails for merged batches only, on a real CUDA GPU.

CUDA-touching variant of the torch-free fixture of the same name in
`python/tests/inferio_worker/fixture_impls/`: it holds one CUDA tensor for the
model's lifetime, which is what makes the worker report a `gpu_uuid` and a
`base_mb` and the ledger price it.

Config keys (registry TOML, passed as **kwargs):
  message: the ValueError text, `{n}` = batch size (default "refusing merged
           batch of {n}"). A second registration uses a message that merely
           contains "out of memory", to probe the host's substring match.
  load_mb: MiB held for the model's lifetime (default 64).
  device:  torch device string (default "cuda"); the worker is pinned with
           CUDA_VISIBLE_DEVICES, so "cuda" is always the intended GPU.

See tools/calibration-protocol/fixtures/README.md "Why a CUDA-touching
variant exists".
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
