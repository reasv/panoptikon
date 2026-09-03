"""Fixture impl that OOMs for the first N seconds after load, then recovers.

`oom_cuda_impl.py` OOMs forever, which measures how far `deflation` climbs
(finding B8: unbounded) but can never measure the other half of the question
the protocol asks in §4 S5 — **how long recovery takes once the OOMs stop**.
Deflation recovers one level per three clean windows, so a model that has
deflated to N needs ~3N clean windows to come back; nothing in the shipped
fixture set can produce that transition on one resident worker.

This fixture does: every `predict` raises the classified batch-1 OOM error
until `oom_secs` have elapsed since `load()`, and succeeds after that. The
worker is never killed and the profile is never reloaded, so the deflation
counter that climbed during the OOM phase is the same one whose recovery is
timed during the healthy phase.

Config keys (from the registry TOML, passed as **kwargs):
  oom_secs: seconds after load during which every predict OOMs (default 120).
  load_mb:  MiB of device memory to hold for the model's lifetime (default 64).
  device:   torch device string (default "cuda").

Keep this stdlib+torch only and self-contained: the worker's discovery
(`inferio_worker/discovery.py`) loads each file as a standalone module, so
relative imports between fixture files do not work.
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
