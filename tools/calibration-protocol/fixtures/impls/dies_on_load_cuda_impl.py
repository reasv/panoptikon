"""Fixture impl that fails in `load()`, every time, on a real CUDA board.

Companion to `dying_cuda_impl.py` (which dies *mid-predict*). This one never
becomes resident: `load()` initialises CUDA, touches one tensor so the load
window looks like a real load to `memory.py`, and then raises. It is the
S5 half of `docs/batch-calibration-test-protocol.md` §4 that measures the
respawn cadence and how long a request stream takes to fail when a model can
never come up (finding B15: no backoff, no attempt cap on respawn; each
attempt can cost up to `load_secs`).

The raise happens *after* the allocation on purpose: a load that dies before
touching the device would exercise the "no gpu_uuid" path instead, which is
`dying_cuda_impl.py`'s problem, not this one's.

Config keys (from the registry TOML, passed as **kwargs):
  load_mb:   MiB of device memory to touch before failing (default 64).
  device:    torch device string (default "cuda").
  load_delay_secs: seconds to sleep before raising (default 0) — set it to
             make each failed attempt cost measurable wall time.

Keep this stdlib+torch only and self-contained: the worker's discovery
(`inferio_worker/discovery.py`) loads each file as a standalone module, so
relative imports between fixture files do not work.
"""

import time

import torch


class DiesOnLoadCudaModel:
    def __init__(self, **config):
        self.config = config
        self.load_mb = int(config.get("load_mb", 64))
        self.device = str(config.get("device", "cuda"))
        self.load_delay_secs = float(config.get("load_delay_secs", 0.0))
        self._ballast = None

    def predict(self, inputs):
        # Unreachable: the model never loads.
        return [{"batch": len(inputs)} for _ in inputs]

    @classmethod
    def name(cls) -> str:
        return "dies_on_load_cuda_test"

    def load(self) -> None:
        if not torch.cuda.is_available():
            raise RuntimeError(
                "dies_on_load_cuda_test requires CUDA: torch.cuda.is_available() is False"
            )
        elems = max(1, (self.load_mb * 1024 * 1024) // 4)
        self._ballast = torch.empty(elems, dtype=torch.float32, device=self.device)
        self._ballast.fill_(1.0)
        torch.cuda.synchronize()
        if self.load_delay_secs > 0:
            time.sleep(self.load_delay_secs)
        self._ballast = None
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass
        raise RuntimeError("dies_on_load_cuda_test: deliberate load failure (fixture)")

    def unload(self) -> None:
        self._ballast = None
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


IMPL_CLASS = DiesOnLoadCudaModel
