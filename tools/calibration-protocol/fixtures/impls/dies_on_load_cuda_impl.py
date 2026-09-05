"""Fixture impl that fails in `load()`, every time, on a real CUDA GPU.

`load()` touches one tensor and only then raises, so the load window looks
like a real load to `memory.py`; failing before touching the device would
exercise the no-`gpu_uuid` path instead. S5 uses it to measure the respawn
cadence when a model can never come up.

Config keys (registry TOML, passed as **kwargs):
  load_mb:         MiB touched before failing (default 64).
  device:          torch device string (default "cuda").
  load_delay_secs: sleep before raising (default 0), to give each failed
                   attempt measurable wall time.

See tools/calibration-protocol/fixtures/README.md "Why a CUDA-touching
variant exists".
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
