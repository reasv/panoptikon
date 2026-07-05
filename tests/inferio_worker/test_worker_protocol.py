"""End-to-end tests for the `inferio_worker` harness.

Each test spawns `python -m inferio_worker` as a real subprocess and speaks
the framed-msgpack protocol from docs/inferio-worker-protocol.md over its
stdin/stdout, exactly like the Rust orchestrator will.

The worker subprocess resolves the `inferio_worker` package via
PYTHONPATH=src (the repo uses a src/ layout; the root conftest only patches
sys.path for the *test* process, which a subprocess does not inherit).
"""

from __future__ import annotations

import os
import struct
import subprocess
import sys
import threading
from pathlib import Path

import msgpack
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
FIXTURE_DIR = Path(__file__).resolve().parent / "fixture_impls"

READ_TIMEOUT = 60.0


class WorkerProcess:
    """Drives one worker subprocess over the framed protocol."""

    def __init__(self) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(SRC_DIR) + os.pathsep + env.get(
            "PYTHONPATH", ""
        )
        # Keep worker startup deterministic in tests: no CUDA path probing
        # (which would import torch if present in the venv).
        env["NO_CUDNN"] = "true"
        env["INFERIO_WORKER"] = "1"
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "inferio_worker"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=str(REPO_ROOT),
        )
        self._stderr_lines: list[str] = []
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, daemon=True
        )
        self._stderr_thread.start()

    def _drain_stderr(self) -> None:
        assert self.proc.stderr is not None
        for line in self.proc.stderr:
            self._stderr_lines.append(
                line.decode("utf-8", errors="replace").rstrip()
            )

    @property
    def stderr_text(self) -> str:
        return "\n".join(self._stderr_lines)

    def send(self, message: dict) -> None:
        payload = msgpack.packb(message, use_bin_type=True)
        assert self.proc.stdin is not None
        self.proc.stdin.write(struct.pack("<I", len(payload)) + payload)
        self.proc.stdin.flush()

    def recv(self, timeout: float = READ_TIMEOUT) -> dict:
        """Read one response frame, failing (not hanging) on a dead worker."""
        result: list[dict] = []
        error: list[BaseException] = []

        def _read() -> None:
            try:
                header = self._read_exact(4)
                (length,) = struct.unpack("<I", header)
                payload = self._read_exact(length)
                result.append(msgpack.unpackb(payload, raw=False))
            except BaseException as e:  # surfaced in the main thread
                error.append(e)

        t = threading.Thread(target=_read, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            self.kill()
            pytest.fail(
                f"Timed out waiting for a frame. Worker stderr:\n"
                f"{self.stderr_text}"
            )
        if error:
            pytest.fail(
                f"Failed to read a frame: {error[0]!r}. Worker stderr:\n"
                f"{self.stderr_text}"
            )
        return result[0]

    def _read_exact(self, size: int) -> bytes:
        assert self.proc.stdout is not None
        buf = bytearray()
        while len(buf) < size:
            chunk = self.proc.stdout.read(size - len(buf))
            if not chunk:
                raise EOFError(
                    f"worker stdout closed after {len(buf)}/{size} bytes"
                )
            buf += chunk
        return bytes(buf)

    def wait(self, timeout: float = READ_TIMEOUT) -> int:
        code = self.proc.wait(timeout=timeout)
        self._stderr_thread.join(timeout=10)
        return code

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)


@pytest.fixture()
def worker():
    w = WorkerProcess()
    try:
        yield w
    finally:
        w.kill()


def handshake_msg(
    req_id: int = 1, impl_class: str = "echo_test", config: dict | None = None
) -> dict:
    return {
        "type": "handshake",
        "id": req_id,
        "protocol_version": 1,
        "inference_id": "test/worker",
        "impl_class": impl_class,
        "config": config or {},
        "impl_dirs": [str(FIXTURE_DIR)],
    }


def test_full_lifecycle_happy_path(worker: WorkerProcess) -> None:
    # Expected behavior: handshake resolves EchoModel by its name() in the
    # fixture impl dir and replies ok with protocol_version=1; load replies
    # ok; predict returns one output per input, in order, with bytes outputs
    # as msgpack bin (round-tripping to Python bytes) and JSON-like outputs
    # as plain msgpack values; unload replies ok and the worker exits 0.
    worker.send(handshake_msg(req_id=1))
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 1
    assert resp["protocol_version"] == 1

    worker.send({"type": "load", "id": 2})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 2

    inputs = [
        {"data": {"text": "hello"}, "file": None},
        {"data": None, "file": b"\x00\x01\xfe\xff"},
    ]
    worker.send({"type": "predict", "id": 3, "inputs": inputs})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 3
    outputs = resp["outputs"]
    assert len(outputs) == 2
    # Data-only input: the JSON-like dict comes back as a msgpack map.
    assert outputs[0] == {"echo": {"text": "hello"}}
    # File input: bytes output stays bytes (msgpack bin, not str).
    assert isinstance(outputs[1], bytes)
    assert outputs[1] == b"echo:\x00\x01\xfe\xff"

    worker.send({"type": "unload", "id": 4})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 4
    assert worker.wait() == 0


def test_load_is_idempotent(worker: WorkerProcess) -> None:
    # Expected behavior: a repeated load replies ok without error (the
    # impl's own load() guard makes it a no-op), matching today's
    # InferenceModel.load() guard semantics.
    worker.send(handshake_msg(req_id=1))
    assert worker.recv()["type"] == "ok"
    worker.send({"type": "load", "id": 2})
    assert worker.recv()["type"] == "ok"
    worker.send({"type": "load", "id": 3})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 3


def test_handshake_unknown_impl_class(worker: WorkerProcess) -> None:
    # Expected behavior: a handshake naming an impl_class that no module in
    # impl_dirs provides gets an error frame (with a traceback) and the
    # worker then exits non-zero — a failed handshake is the one error the
    # worker does not survive.
    worker.send(handshake_msg(req_id=1, impl_class="does_not_exist"))
    resp = worker.recv()
    assert resp["type"] == "error"
    assert resp["id"] == 1
    assert "does_not_exist" in resp["message"]
    assert isinstance(resp["traceback"], str)
    assert worker.wait() != 0


def test_predict_before_load_is_error_and_worker_survives(
    worker: WorkerProcess,
) -> None:
    # Expected behavior: predict without a prior successful load replies
    # error (sanity check per the protocol doc), and the worker stays alive
    # and serviceable — a follow-up ping replies ok.
    worker.send(handshake_msg(req_id=1))
    assert worker.recv()["type"] == "ok"

    worker.send(
        {"type": "predict", "id": 2, "inputs": [{"data": "x", "file": None}]}
    )
    resp = worker.recv()
    assert resp["type"] == "error"
    assert resp["id"] == 2
    assert "load" in resp["message"]

    worker.send({"type": "ping", "id": 3})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 3


def test_stdout_hygiene_survives_prints(worker: WorkerProcess) -> None:
    # Expected behavior: an impl that print()s during load/predict/unload
    # cannot corrupt the protocol stream — fd 1 is redirected to stderr and
    # sys.stdout is rebound before impl code ever runs, so every frame still
    # parses and the printed garbage shows up on stderr instead.
    worker.send(handshake_msg(req_id=1, impl_class="printing_test"))
    assert worker.recv()["type"] == "ok"

    worker.send({"type": "load", "id": 2})
    assert worker.recv()["type"] == "ok"

    worker.send(
        {
            "type": "predict",
            "id": 3,
            "inputs": [{"data": 1, "file": None}, {"data": 2, "file": None}],
        }
    )
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["outputs"] == [{"printed": True}, {"printed": True}]

    worker.send({"type": "unload", "id": 4})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0
    # All three print() outputs (load/predict/unload) were rerouted to
    # stderr, not lost and not on stdout.
    assert "garbage on load stdout" in worker.stderr_text
    assert "garbage on predict stdout" in worker.stderr_text
    assert "garbage on unload stdout" in worker.stderr_text


def test_unknown_request_type_and_prewarm_are_unsupported(
    worker: WorkerProcess,
) -> None:
    # Expected behavior: an unknown request type replies error with
    # "unsupported" in the message and the worker keeps serving; the
    # reserved prewarm type behaves the same in v1.
    worker.send(handshake_msg(req_id=1))
    assert worker.recv()["type"] == "ok"

    worker.send({"type": "frobnicate", "id": 2})
    resp = worker.recv()
    assert resp["type"] == "error"
    assert resp["id"] == 2
    assert "unsupported" in resp["message"]

    worker.send({"type": "prewarm", "id": 3})
    resp = worker.recv()
    assert resp["type"] == "error"
    assert resp["id"] == 3
    assert "unsupported" in resp["message"]

    worker.send({"type": "ping", "id": 4})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 4


def test_broken_module_does_not_prevent_discovery(
    worker: WorkerProcess,
) -> None:
    # Expected behavior: broken_impl.py (raises at import time, and sorts
    # before echo_impl.py so discovery hits it first) is logged as a warning
    # and skipped; echo_test is still found and the handshake succeeds —
    # mirroring get_impl_classes' tolerance for unrelated broken modules.
    worker.send(handshake_msg(req_id=1, impl_class="echo_test"))
    resp = worker.recv()
    assert resp["type"] == "ok"

    worker.send({"type": "unload", "id": 2})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0
    assert "broken_impl" in worker.stderr_text
