"""Test fixture impl that writes hostile bytes straight to the stderr FD.

Exercises the orchestrator's stderr forwarder: predict() emits raw invalid
UTF-8 (a cp1252-ish byte soup) and a >64 KiB carriage-return-only run
(tqdm-style progress with no newlines) via os.write(2, ...), bypassing
PYTHONIOENCODING and Python's text layer entirely. The forwarder must keep
reading through all of it — if it dies, the pipe fills and the worker
blocks mid-predict.
"""

import os


class BadBytesModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "badbytes_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        # Invalid UTF-8 on its own line: the old lines()-based forwarder
        # died permanently on the first such byte.
        os.write(2, b"bad \xff\xfe\x9d\x81 bytes\n")
        # A \r-only run well past the 64 KiB single-line cap (9 bytes x
        # 10000 = ~88 KiB): no newline ever arrives, so an uncapped line
        # reader would grow unboundedly and a dead one would wedge the pipe.
        os.write(2, b"\rprogress" * 10000)
        # Marker after the garbage proves the forwarder is still reading.
        os.write(2, b"marker-after-bad-bytes\n")
        return [{"bad": True} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = BadBytesModel
