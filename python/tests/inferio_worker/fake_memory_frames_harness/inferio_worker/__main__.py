"""A fake inferio worker that writes whatever the mid-request stream test
under examination needs, and nothing else.

The real harness only emits per-batch `memory` frames when it has an
accelerator to measure, so it cannot drive the orchestrator's reader on a
test host. This fake speaks the framing directly and writes the exact
sequence named by INFERIO_FAKE_FRAMES before each `predict` reply:

    memory   N `memory` frames for the request in flight, each carrying a
             sample whose `reserved_mb` is the frame's 1-based index * 100
             (default N=2)
    foreign  one `memory` frame for a *different* id — a desynchronized
             stream, which must stay fatal
    unknown  one frame of an unrecognized type for the request in flight —
             likewise fatal, exactly as before the reader loop existed
    silent   nothing at all, whatever the handshake asked for: an
             orchestrator's-eye view of a worker predating the capability
    require  as `memory`, but the handshake FAILS unless the orchestrator
             announced `batch_memory_frames: true` — so a spawn that
             succeeds is the assertion that it did

INFERIO_FAKE_REPLY_MEMORY=0 drops the response-level sample from the reply,
which is how a test reads what the frames alone left behind.
See panoptikon/src/inferio/worker.rs's per-batch memory frame tests.
"""

import os
import struct
import sys

import msgpack

FREE_MB = 4096
TOTAL_MB = 8192


def _read_exact(stream, size: int) -> bytes:
    buf = bytearray()
    while len(buf) < size:
        chunk = stream.read(size - len(buf))
        if not chunk:
            raise SystemExit(0)
        buf += chunk
    return bytes(buf)


def _read_frame(stream) -> dict:
    (length,) = struct.unpack("<I", _read_exact(stream, 4))
    return msgpack.unpackb(_read_exact(stream, length), raw=False)


def _write_frame(stream, message: dict) -> None:
    payload = msgpack.packb(message, use_bin_type=True)
    stream.write(struct.pack("<I", len(payload)) + payload)
    stream.flush()


def _sample(reserved_mb: int) -> dict:
    return {
        "free_mb": FREE_MB - reserved_mb,
        "total_mb": TOTAL_MB,
        "free_source": "nvml",
        "reserved_mb": reserved_mb,
        "allocated_mb": reserved_mb,
    }


def _mid_request_frames(out, req_id: int, mode: str, count: int) -> None:
    if mode == "silent":
        return
    if mode == "require":
        mode = "memory"
    if mode == "foreign":
        _write_frame(out, {"type": "memory", "id": req_id + 1000,
                           "memory": _sample(100)})
        return
    if mode == "unknown":
        _write_frame(out, {"type": "progress", "id": req_id, "done": 1})
        return
    for index in range(1, count + 1):
        _write_frame(out, {"type": "memory", "id": req_id,
                           "memory": _sample(100 * index)})


def main() -> None:
    mode = os.getenv("INFERIO_FAKE_FRAMES", "memory")
    count = int(os.getenv("INFERIO_FAKE_FRAME_COUNT", "2"))
    out_fd = os.dup(1)
    os.dup2(2, 1)
    in_fd = os.dup(0)
    if sys.platform == "win32":
        import msvcrt

        msvcrt.setmode(out_fd, os.O_BINARY)
        msvcrt.setmode(in_fd, os.O_BINARY)
    proto_in = os.fdopen(in_fd, "rb", buffering=0)
    proto_out = os.fdopen(out_fd, "wb", buffering=0)

    while True:
        msg = _read_frame(proto_in)
        req_id = msg.get("id", 0)
        mtype = msg.get("type")
        if mtype == "handshake":
            if mode == "require" and msg.get("batch_memory_frames") is not True:
                _write_frame(
                    proto_out,
                    {
                        "type": "error",
                        "id": req_id,
                        "message": "handshake did not announce "
                        "batch_memory_frames",
                        "traceback": "",
                    },
                )
                raise SystemExit(1)
            _write_frame(
                proto_out,
                {"type": "ok", "id": req_id, "protocol_version": 2},
            )
        elif mtype == "predict":
            _mid_request_frames(proto_out, req_id, mode, count)
            inputs = msg.get("inputs") or []
            reply = {
                "type": "ok",
                "id": req_id,
                "outputs": [{"echo": index} for index in range(len(inputs))],
            }
            if os.getenv("INFERIO_FAKE_REPLY_MEMORY") != "0":
                reply["memory"] = _sample(100 * (count + 1))
            _write_frame(proto_out, reply)
        elif mtype == "unload":
            _write_frame(proto_out, {"type": "ok", "id": req_id})
            return
        else:
            _write_frame(proto_out, {"type": "ok", "id": req_id})


if __name__ == "__main__":
    main()
