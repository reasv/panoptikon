"""Fixture impl that OOMs for the first N seconds after load, then recovers.

Every predict raises the classified batch-1 OOM error until `oom_secs` have
elapsed since `load()`, and succeeds after that. The worker is never killed
and the profile is never reloaded, so the deflation counter that climbed
during the OOM phase is the one whose recovery is timed afterwards -- which
is what `oom_cuda_impl.py` (OOMs forever) cannot measure.

Config keys (registry TOML, passed as **kwargs):
  oom_secs: seconds after load during which every predict OOMs (default 120).
  load_mb:  MiB held for the model's lifetime (default 64).
  device:   torch device string (default "cuda").

See tools/calibration-protocol/fixtures/README.md "Why a CUDA-touching
variant exists".
"""

import time

import torch


class OomTimedCudaModel:
    def __init__(self, **config):
        self.config = config
        self.load_mb = int(config.get("load_mb", 64))
        self.device = str(config.get("device", "cuda"))
        self.oom_secs = float(config.get("oom_secs", 120.0))
        self._loaded_at = None
        self._ballast = None

    def predict(self, inputs):
        started = self._loaded_at if self._loaded_at is not None else time.monotonic()
        if time.monotonic() - started < self.oom_secs:
            raise RuntimeError(
                "INFERENCE_OOM_BATCH_SIZE_1: fixture single-item OOM (timed)"
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
        self._loaded_at = time.monotonic()

    def unload(self) -> None:
        self._ballast = None
        self._loaded_at = None
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


IMPL_CLASS = OomTimedCudaModel
