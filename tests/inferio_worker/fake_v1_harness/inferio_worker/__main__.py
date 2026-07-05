"""A fake, stale (v1) inferio worker harness.

Speaks just enough of the framed-msgpack protocol to answer the handshake
with `ok` + `protocol_version: 1`, then lingers on stdin. The Rust
orchestrator must reject the echoed version and kill the process — this
fake exists solely so that check can be integration-tested against a live
child (gateway/src/inferio/worker.rs::version_mismatch_kills_worker).
"""

import os
import struct
import sys

import msgpack


def _read_exact(stream, size: int) -> bytes:
    buf = bytearray()
    while len(buf) < size:
        chunk = stream.read(size - len(buf))
        if not chunk:
            raise SystemExit(2)
        buf += chunk
    return bytes(buf)


def main() -> None:
    out_fd = os.dup(1)
    os.dup2(2, 1)
    in_fd = os.dup(0)
    if sys.platform == "win32":
        import msvcrt

        msvcrt.setmode(out_fd, os.O_BINARY)
        msvcrt.setmode(in_fd, os.O_BINARY)
    proto_in = os.fdopen(in_fd, "rb", buffering=0)
    proto_out = os.fdopen(out_fd, "wb", buffering=0)

    header = _read_exact(proto_in, 4)
    (length,) = struct.unpack("<I", header)
    request = msgpack.unpackb(_read_exact(proto_in, length), raw=False)
    payload = msgpack.packb(
        {"type": "ok", "id": request.get("id", 0), "protocol_version": 1},
        use_bin_type=True,
    )
    proto_out.write(struct.pack("<I", len(payload)) + payload)
    proto_out.flush()
    # Linger: the orchestrator (not our own exit) must decide the outcome.
    proto_in.read()


if __name__ == "__main__":
    main()
