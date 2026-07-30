"""Test fixture impl that OOMs on its *second* GPU batch.

Exercises the packing harness's failure path end to end: with a grant the
harness splits a window into several batches, so the first one succeeds and is
measured, the second raises a driver-shaped out-of-memory error, and the
window fails as a whole while still reporting both measurements (the second
flagged `oom`) on the error frame. Torch-free — the message is the bare driver
text, which is exactly the case the orchestrator's substring classification
exists for.
"""


class OomSecondBatchModel:
    def __init__(self, **config):
        self.config = config
        self.batches = 0

    @classmethod
    def name(cls) -> str:
        return "oom_second_batch_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        self.batches += 1
        if self.batches >= 2:
            raise RuntimeError(
                "CUDA out of memory. Tried to allocate 2.00 GiB (fixture)"
            )
        return [{"batch": len(inputs)} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = OomSecondBatchModel
