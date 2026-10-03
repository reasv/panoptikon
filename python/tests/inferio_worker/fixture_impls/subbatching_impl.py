"""Test fixture impl that sub-batches *inside* predict, like shipped impls do.

`run_with_oom_retry(..., initial_chunk_size=1)` is the shape florence2 uses and
the shape moondream/dots_ocr reach when their own chunk size is smaller than the
batch handed over. The GPU batch the allocator actually saw is then one item,
not the packed window slice — so the packing harness must report that batch
*unpriced* rather than attributing the packed unit count to a one-item peak
(docs/inferio-worker-protocol.md, "Memory sensing").

Torch-free: `oom_exceptions` is overridden so `run_with_oom_retry` never
imports torch. `inferio.impl.utils` is imported lazily inside `predict` rather
than at module level, because impl discovery imports every module in this
directory and the other fixtures must not pay for numpy/PIL.
"""


class SubBatchingModel:
    def __init__(self, **config):
        self.config = config
        self.chunks: list[int] = []

    @classmethod
    def name(cls) -> str:
        return "subbatching_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        from inferio.impl.utils import run_with_oom_retry

        def process(chunk):
            self.chunks.append(len(chunk))
            return [{"chunk": len(chunk)} for _ in chunk]

        return run_with_oom_retry(
            process,
            list(inputs),
            initial_chunk_size=1,
            oom_exceptions=(RuntimeError,),
        )

    def unload(self) -> None:
        pass


IMPL_CLASS = SubBatchingModel
