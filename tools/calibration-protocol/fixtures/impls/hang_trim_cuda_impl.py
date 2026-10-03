"""Fixture impl that ignores an orchestrator trim for longer than TRIM_DEADLINE.

The trim round trip has a fixed, fatal deadline (`worker.rs: TRIM_DEADLINE`)
and anything that is not a per-request `WorkerError` drops the whole model,
not just the trim. This fixture makes that deadline expire on purpose.

How it hangs: the worker's trim arm calls `memory.empty_cache()` and only then
replies, on a single-threaded loop, so a slow `empty_cache()` is a slow trim.
`load()` rebinds `inferio_worker.memory.empty_cache` to a sleeping version;
`__main__` resolves the attribute at call time, so the rebind takes effect.
The reactive shrink (`packing.py: maybe_shrink`) calls the same function from
inside a predict, so the wrapper sleeps only when no predict is in flight.

To be flagged for a trim at all, `flag_trims_locked` (`ledger.rs`) wants pool
growth of at least `TRIM_SLACK_MB` since load on an idle, grantless replica --
hence the transient `pool_mb` tensor each predict allocates and drops, which
the caching allocator keeps as reserved pool.

Config keys (registry TOML, passed as **kwargs):
  hang_secs: seconds to sleep inside a trim (default 70, past the deadline;
             below it for the control leg where the trim succeeds late).
  pool_mb:   MiB allocated and freed per predict, to create pool slack
             (default 512, twice TRIM_SLACK_MB).
  load_mb:   MiB held for the model's lifetime (default 64).
  device:    torch device string (default "cuda").

See tools/calibration-protocol/fixtures/README.md "Why a CUDA-touching
variant exists".
"""

import logging
import threading
import time

import torch

logger = logging.getLogger("inferio_worker")


class HangTrimCudaModel:
    def __init__(self, **config):
        self.config = config
        self.load_mb = int(config.get("load_mb", 64))
        self.pool_mb = int(config.get("pool_mb", 512))
        self.hang_secs = float(config.get("hang_secs", 70))
        self.device = str(config.get("device", "cuda"))
        self._ballast = None
        self._in_predict = threading.Event()

    def _install_trim_hang(self) -> None:
        from inferio_worker import memory

        original = getattr(memory, "_hang_trim_original_empty_cache", None)
        if original is None:
            original = memory.empty_cache
            memory._hang_trim_original_empty_cache = original
        in_predict = self._in_predict
        hang_secs = self.hang_secs

        def slow_empty_cache() -> bool:
            # Only the orchestrator's trim arrives with no predict in flight.
            if not in_predict.is_set():
                logger.info(
                    "hang_trim_cuda_test - ignoring the trim for %.1fs", hang_secs
                )
                time.sleep(hang_secs)
                logger.info("hang_trim_cuda_test - done ignoring the trim")
            return original()

        memory.empty_cache = slow_empty_cache

    def predict(self, inputs):
        self._in_predict.set()
        try:
            elems = max(1, (self.pool_mb * 1024 * 1024) // 4)
            scratch = torch.empty(elems, dtype=torch.float32, device=self.device)
            scratch.fill_(1.0)
            torch.cuda.synchronize()
            del scratch
            time.sleep(0.05)
            return [{"ok": True} for _ in inputs]
        finally:
            self._in_predict.clear()

    @classmethod
    def name(cls) -> str:
        return "hang_trim_cuda_test"

    def load(self) -> None:
        if not torch.cuda.is_available():
            raise RuntimeError(
                "hang_trim_cuda_test requires CUDA: torch.cuda.is_available() is False"
            )
        elems = max(1, (self.load_mb * 1024 * 1024) // 4)
        self._ballast = torch.empty(elems, dtype=torch.float32, device=self.device)
        self._ballast.fill_(1.0)
        torch.cuda.synchronize()
        self._install_trim_hang()

    def unload(self) -> None:
        self._ballast = None
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


IMPL_CLASS = HangTrimCudaModel
