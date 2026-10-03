"""A fake v2 inferio worker that answers `trim` with a per-request `error`.

The real harness can never do this — a trim that releases nothing is still a
successful trim, by design — but an *older* worker can, and must: a v2
orchestrator talking to a harness that predates the `trim` request type gets
"unsupported request type" back, and the protocol says an unknown `type` is a
per-request error the worker survives. That is precisely why adding `trim` did
not need a protocol version bump, so the orchestrator's handling of it needs a
live worker that actually behaves that way.

What the Rust side must do with it: treat the failure as *nothing at all*. The
trim was hygiene, the exchange completed, the stream is in sync, and the
replica goes straight back into the free pool still serving predicts. Only a
**fatal** error (a desynchronized stream, a dead process) may cost a replica.

Everything else is answered minimally and honestly enough to get a replica
loaded and serving: handshake echoes protocol_version 2, configure/load/ping
answer `ok`, and predict echoes each input's `data` the way the `echo_test`
fixture impl does, so a test can assert the worker is still usable afterwards.
"""

import os
import struct
import sys

import msgpack

PROTOCOL_VERSION = 2


def _read_exact(stream, size: int) -> bytes:
    buf = bytearray()
    while len(buf) < size:
        chunk = stream.read(size - len(buf))
        if not chunk:
            raise EOFError
        buf += chunk
    return bytes(buf)


def _read_frame(stream):
    try:
        header = _read_exact(stream, 4)
    except EOFError:
        return None
    (length,) = struct.unpack("<I", header)
    return msgpack.unpackb(_read_exact(stream, length), raw=False)


def _write_frame(stream, message: dict) -> None:
    payload = msgpack.packb(message, use_bin_type=True)
    stream.write(struct.pack("<I", len(payload)) + payload)
    stream.flush()


def main() -> int:
    # The same stdout-hygiene dance the real harness performs: fd 1 becomes
    # the protocol channel and stderr takes its place, so a stray print can
    # never corrupt a frame.
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
        message = _read_frame(proto_in)
        if message is None:
            return 0
        req_id = message.get("id", 0)
        mtype = message.get("type")
        if mtype == "handshake":
            _write_frame(
                proto_out,
                {
                    "type": "ok",
                    "id": req_id,
                    "protocol_version": PROTOCOL_VERSION,
                },
            )
        elif mtype == "trim":
            # What an older harness says about a request type it never heard
            # of. The worker stays alive and the stream stays in sync.
            _write_frame(
                proto_out,
                {
                    "type": "error",
                    "id": req_id,
                    "message": "unsupported request type: 'trim'",
                    "traceback": "",
                },
            )
        elif mtype == "predict":
            outputs = [
                {"echo": entry.get("data")} for entry in message.get("inputs") or []
            ]
            _write_frame(
                proto_out, {"type": "ok", "id": req_id, "outputs": outputs}
            )
        elif mtype == "unload":
            _write_frame(proto_out, {"type": "ok", "id": req_id})
            return 0
        else:
            # configure, load, prewarm, ping.
            _write_frame(proto_out, {"type": "ok", "id": req_id})


if __name__ == "__main__":
    raise SystemExit(main())
