"""Framing for the inferio worker protocol v1.

Frame = 4-byte little-endian u32 payload length, then exactly that many
bytes of a single msgpack-encoded map. See docs/inferio-worker-protocol.md.
"""

from __future__ import annotations

import struct
from typing import Any, BinaryIO

import msgpack

PROTOCOL_VERSION = 1
# Max frame size 512 MiB; a larger declared length is a fatal protocol error.
MAX_FRAME_BYTES = 0x2000_0000


class ProtocolError(Exception):
    """Fatal framing violation. The worker must exit non-zero."""


def _msgpack_default(obj: Any) -> Any:
    """Fallback for values msgpack cannot serialize natively.

    Impl `predict()` is contracted to return `bytes | dict | list | str`
    (see `inferio.model.InferenceModel`); today's HTTP layer rejects anything
    else. Numpy containers/scalars are duck-converted (no numpy import here)
    so a stray np.float32 inside a dict degrades gracefully instead of
    poisoning the frame; anything else is a TypeError, which the caller
    surfaces as an `error` frame for that request.
    """
    if hasattr(obj, "dtype"):
        if getattr(obj, "shape", None) == () and callable(
            getattr(obj, "item", None)
        ):
            return obj.item()
        if callable(getattr(obj, "tolist", None)):
            return obj.tolist()
    raise TypeError(
        f"Object of type {type(obj).__name__} is not msgpack-serializable"
    )


def _read_exact(stream: BinaryIO, size: int) -> bytes:
    buf = bytearray()
    while len(buf) < size:
        chunk = stream.read(size - len(buf))
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


def read_frame(stream: BinaryIO) -> dict | None:
    """Read one frame. Returns None on clean EOF at a frame boundary."""
    header = _read_exact(stream, 4)
    if len(header) == 0:
        return None
    if len(header) < 4:
        raise ProtocolError("EOF in the middle of a frame header")
    (length,) = struct.unpack("<I", header)
    if length > MAX_FRAME_BYTES:
        raise ProtocolError(
            f"Declared frame length {length} exceeds the {MAX_FRAME_BYTES} limit"
        )
    payload = _read_exact(stream, length)
    if len(payload) < length:
        raise ProtocolError("EOF in the middle of a frame payload")
    try:
        message = msgpack.unpackb(payload, raw=False)
    except Exception as e:
        raise ProtocolError(f"Frame payload is not valid msgpack: {e}") from e
    if not isinstance(message, dict):
        raise ProtocolError("Frame payload is not a msgpack map")
    return message


def write_frame(stream: BinaryIO, message: dict) -> None:
    """Serialize and write one frame.

    Packing happens fully before any byte is written, so a serialization
    failure never corrupts the stream (callers catch it and send an `error`
    frame instead).
    """
    payload = msgpack.packb(
        message, use_bin_type=True, default=_msgpack_default
    )
    if len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError(
            f"Refusing to write frame of {len(payload)} bytes (over the "
            f"{MAX_FRAME_BYTES} limit)"
        )
    stream.write(struct.pack("<I", len(payload)) + payload)
    stream.flush()
