"""Fixture impl that OOMs on its first N predicts, then recovers.

The first `oom_predicts` predicts raise the classified batch-1 OOM error, and
every later one succeeds. The worker is never killed and the profile is never
reloaded, so the deflation counter that climbed during the OOM phase is the
one whose recovery is timed afterwards -- which is what `oom_cuda_impl.py`
(OOMs forever) cannot measure. A count, not a time, so every job longer than
the OOM phase records the recovery, however fast the host runs it.

Config keys (registry TOML, passed as **kwargs):
  oom_predicts: how many predicts OOM, from the first (default 20).
  load_mb:      MiB held for the model's lifetime (default 64).
  device:       torch device string (default "cuda").

See tools/calibration-protocol/fixtures/README.md "Why a CUDA-touching
variant exists".
"""

import torch


class OomTimedCudaModel:
    def __init__(self, **config):
        self.config = config
        self.load_mb = int(config.get("load_mb", 64))
        self.device = str(config.get("device", "cuda"))
        self.oom_predicts = int(config.get("oom_predicts", 20))
        self.predicts = 0
        self._ballast = None

    def predict(self, inputs):
        self.predicts += 1
        if self.predicts <= self.oom_predicts:
            raise RuntimeError(
                "INFERENCE_OOM_BATCH_SIZE_1: fixture single-item OOM (first predicts)"
            )
        return [{"batch": len(inputs)} for _ in inputs]

    @classmethod
    def name(cls) -> str:
        return "oom_timed_cuda_test"

    def load(self) -> None:
        if not torch.cuda.is_available():
            raise RuntimeError(
                "oom_timed_cuda_test requires CUDA: torch.cuda.is_available() is False"
            )
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


IMPL_CLASS = OomTimedCudaModel
