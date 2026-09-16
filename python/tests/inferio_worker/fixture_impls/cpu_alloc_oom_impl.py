"""Test fixture impl that raises torch's host-RAM allocation failure.

The CPU-priced Mac leg (tools/calibration-protocol/README.md, the MPS pass):
the text is what `DefaultCPUAllocator` raises when the *host* is out of
memory, so the free figure the report is weighed against must be RAM, not
Metal's headroom. Torch-free, and it allocates nothing.
"""


class CpuAllocOomModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "cpu_alloc_oom_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        raise RuntimeError(
            "[enforce fail at alloc_cpu.cpp:117] data. DefaultCPUAllocator: "
            "can't allocate memory: you tried to allocate 8589934592 bytes."
        )

    def unload(self) -> None:
        pass


IMPL_CLASS = CpuAllocOomModel
