"""Fixture impl that ignores an orchestrator trim for longer than TRIM_DEADLINE.

Probe for finding **B17** (`docs/batch-calibration-test-protocol.md` §5): the
trim round trip has a fixed, fatal 60 s deadline (`worker.rs: TRIM_DEADLINE`),
and `dispatch.rs: run_trim` turns anything that is not a per-request
`WorkerError` into `BatchOutcome::Fatal` — which drops the whole model, not
just the trim. This fixture makes that deadline expire on purpose.

How it hangs
------------
The worker's trim arm (`inferio_worker/__main__.py`, `mtype == "trim"`) calls
`memory.empty_cache()` and only then replies, and the worker's message loop is
single-threaded, so a slow `empty_cache()` is exactly a slow trim. `load()`
therefore rebinds `inferio_worker.memory.empty_cache` to a version that sleeps
`hang_secs` before delegating to the original. `__main__` resolves the
attribute at call time (`memory.empty_cache()`), so the rebind takes effect.

The **reactive shrink** (`packing.py: maybe_shrink`) calls the same function
from inside a predict, and hanging there would only look like a slow model, so
the wrapper sleeps only when no predict is in flight — i.e. only for the
orchestrator-initiated trim this fixture exists to break.

Getting flagged for a trim at all
---------------------------------
`flag_trims_locked` (`ledger.rs`) only asks a resident to release its pool when
its **pool growth since load** is at least `TRIM_SLACK_MB` (256 MB), it holds
no grant, has no pending requests, settled its last grant at least 5 s ago, and
was not flagged within the last 30 s. So `predict()` allocates a transient
`pool_mb` (default 512) tensor and drops it: the CUDA caching allocator keeps
the freed blocks, `reserved` stays up, and the replica reports pool growth well
past the threshold while sitting idle. A neighbour on the same GPU then has
to be squeezed (a hog plus a fitted model) for the trim to be flagged.

Config keys (from the registry TOML, passed as **kwargs):
  hang_secs: seconds to sleep inside a trim (default 70 — past the 60 s
             deadline; set below 60 for the control leg where the trim
             succeeds late).
  pool_mb:   MiB allocated and freed per predict, to create pool slack
             (default 512, i.e. twice TRIM_SLACK_MB).
  load_mb:   MiB of device memory held for the model's lifetime (default 64).
  device:    torch device string (default "cuda").

Keep this stdlib+torch only and self-contained: the worker's discovery
(`inferio_worker/discovery.py`) loads each file as a standalone module, so
relative imports between fixture files do not work.
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

    # -- the trim hang -----------------------------------------------------
    def _install_trim_hang(self) -> None:
        from inferio_worker import memory

        original = getattr(memory, "_hang_trim_original_empty_cache", None)
        if original is None:
            original = memory.empty_cache
            memory._hang_trim_original_empty_cache = original
        in_predict = self._in_predict
        hang_secs = self.hang_secs

        def slow_empty_cache() -> bool:
            # The reactive shrink calls this from inside a window; only the
            # orchestrator's trim arrives with no predict in flight.
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
