"""Entry point: `python -m inferio_worker`.

Implements the worker side of docs/inferio-worker-protocol.md (v2).
Everything the worker needs arrives in the handshake frame; there are no
command-line arguments. Only stdlib may be imported at module level — the
stdout-hygiene dance must happen before anything beyond the stdlib (msgpack,
impl deps) gets a chance to touch fd 1.

State machine (protocol v2):

    handshaken --prewarm*--> handshaken   (optional prepare(), idempotent)
    handshaken --configure--> configured  (instantiates impl_class(**config))
    configured --load--> loaded
    any state  --unload--> ok + exit 0

- `handshake` carries worker *identity* only (impl_class + impl_dirs); the
  class is located but NOT instantiated, so a prewarmed worker can later be
  claimed for any model of its impl family.
- `prewarm` is valid only between handshake and configure; it calls the
  impl's optional `prepare()` classmethod (absent = no-op → ok). Errors are
  per-request and non-fatal: a failed prepare just means load pays the
  imports.
- `configure` instantiates exactly once (before load); a second configure,
  or load/predict before configure, is a per-request error and the worker
  survives.
- A failed handshake is the one error the worker does not survive (exit
  non-zero).
- The handshake's `batch_memory_frames` flag is the one capability the
  orchestrator announces. With it, a granted `predict` writes a `memory`
  frame carrying the in-flight request id after each batch but the last;
  without it (an older orchestrator) the stream is exactly what it was.
"""

from __future__ import annotations

import logging
import os
import sys
import traceback
from typing import Any, BinaryIO, Callable

EXIT_OK = 0
EXIT_HANDSHAKE_FAILED = 1
EXIT_PROTOCOL_ERROR = 2
EXIT_INTERNAL_ERROR = 3

logger = logging.getLogger("inferio_worker")


def _setup_stdio() -> tuple[BinaryIO, BinaryIO]:
    """Perform the stdout-hygiene dance from the protocol doc.

    1. dup fd 1 -> the protocol channel;
    2. dup2 stderr over fd 1 so stray native/library writes to stdout become
       log lines instead of frame corruption;
    3. rebind sys.stdout to sys.stderr for the same reason at Python level;
    4. binary, unbuffered modes on the protocol fds (O_BINARY on Windows).
    """
    protocol_out_fd = os.dup(1)
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    protocol_in_fd = os.dup(0)
    if sys.platform == "win32":
        import msvcrt

        msvcrt.setmode(protocol_out_fd, os.O_BINARY)
        msvcrt.setmode(protocol_in_fd, os.O_BINARY)
    proto_in = os.fdopen(protocol_in_fd, "rb", buffering=0)
    proto_out = os.fdopen(protocol_out_fd, "wb", buffering=0)
    return proto_in, proto_out


def _send_ok(proto_out: BinaryIO, req_id: int, **payload: Any) -> None:
    from inferio_worker import protocol

    protocol.write_frame(
        proto_out, {"type": "ok", "id": req_id, **payload}
    )


def _send_error(
    proto_out: BinaryIO, req_id: int, message: str, tb: str = "", **extra: Any
) -> None:
    """Send an `error` frame; `extra` carries the optional memory-sensing
    fields a failed `predict` can still report."""
    from inferio_worker import protocol

    protocol.write_frame(
        proto_out,
        {
            "type": "error",
            "id": req_id,
            "message": message,
            "traceback": tb,
            **extra,
        },
    )


def _memory_frame_emitter(
    proto_out: BinaryIO, req_id: int, wanted: bool
) -> Callable[[dict[str, Any]], None] | None:
    """The per-batch `memory` frame writer for one in-flight `predict`, or None
    when the orchestrator did not ask for the frames.

    Bound to `req_id` and handed only to `run_window`, which is the whole of
    the desynchronization argument: a `memory` frame is legal *before* the
    terminal reply for the request now in flight and at no other moment, so
    the emitter cannot outlive the request whose id it carries.
    """
    if not wanted:
        return None

    from inferio_worker import protocol

    def emit(sample: dict[str, Any]) -> None:
        protocol.write_frame(
            proto_out, {"type": "memory", "id": req_id, "memory": sample}
        )

    return emit


def _handshake(
    proto_in: BinaryIO, proto_out: BinaryIO
) -> tuple[type | None, bool]:
    """Process the handshake frame; returns `(impl class, per-batch memory
    frames wanted)`, the class being None on any failure.

    v2: the handshake carries identity only (impl_class + impl_dirs). The
    class is located but not instantiated — `configure` does that later.
    Per the protocol doc, any handshake failure sends an `error` frame and
    the worker exits non-zero (the caller handles the exit).

    `batch_memory_frames` is the one *capability* the handshake carries: an
    orchestrator that sets it reads mid-request `memory` frames instead of
    treating them as a desynchronized stream. It is announced, not agreed —
    absent (an orchestrator predating them) means the frames are never sent,
    which is the whole of the compatibility story in that direction. The
    protocol version is untouched: this is an additive key, and both sides
    ignore unknown ones.
    """
    from inferio_worker import protocol

    msg = protocol.read_frame(proto_in)
    if msg is None:
        logger.error("EOF before handshake; exiting.")
        return None, False
    req_id = msg.get("id", 0)
    if msg.get("type") != "handshake":
        _send_error(
            proto_out,
            req_id,
            f"Expected handshake as first frame, got {msg.get('type')!r}",
        )
        return None, False
    version = msg.get("protocol_version")
    if version != protocol.PROTOCOL_VERSION:
        _send_error(
            proto_out,
            req_id,
            f"Unsupported protocol version {version!r}; this worker speaks "
            f"{protocol.PROTOCOL_VERSION}",
        )
        return None, False
    batch_memory_frames = msg.get("batch_memory_frames") is True

    # cuDNN path setup before any impl module import; failure is only a
    # warning.
    try:
        from inferio_worker.cudnn import cudnn_setup

        cudnn_setup()
    except Exception as e:
        logger.warning("cuDNN setup failed: %s", e, exc_info=True)

    try:
        impl_class_name = msg["impl_class"]
        impl_dirs = msg.get("impl_dirs") or []
        from inferio_worker.discovery import find_impl_class

        impl_cls = find_impl_class(impl_class_name, impl_dirs, logger)
    except Exception as e:
        logger.error("handshake failed: %s", e, exc_info=True)
        _send_error(proto_out, req_id, str(e), traceback.format_exc())
        return None, False

    logger.info("Handshake ok for impl class %s", impl_class_name)
    _send_ok(proto_out, req_id, protocol_version=protocol.PROTOCOL_VERSION)
    return impl_cls, batch_memory_frames


def _serve(proto_in: BinaryIO, proto_out: BinaryIO) -> int:
    from inferio_worker import memory, protocol
    from inferio_worker.inputs import prediction_input_from_frame

    impl_cls, batch_memory_frames = _handshake(proto_in, proto_out)
    if impl_cls is None:
        return EXIT_HANDSHAKE_FAILED

    instance: Any | None = None
    inference_id = "<unconfigured>"
    prewarmed = False
    loaded = False
    batching_off_logged = False
    while True:
        msg = protocol.read_frame(proto_in)
        if msg is None:
            # Parent closed our stdin (orchestrator gone); exit quietly.
            logger.info("stdin EOF; exiting.")
            return EXIT_OK
        req_id = msg.get("id", 0)
        mtype = msg.get("type")

        if mtype == "prewarm":
            # Valid only between handshake and configure; idempotent.
            if instance is not None:
                _send_error(
                    proto_out,
                    req_id,
                    "prewarm after configure is not allowed",
                )
                continue
            if prewarmed:
                _send_ok(proto_out, req_id)
                continue
            prepare = getattr(impl_cls, "prepare", None)
            if prepare is None:
                # No prepare() classmethod: prewarm is a no-op -> plain ok.
                prewarmed = True
                _send_ok(proto_out, req_id)
                continue
            try:
                prepare()
                prewarmed = True
                _send_ok(proto_out, req_id)
            except Exception as e:
                # Per-request and NON-fatal: the worker stays usable — a
                # failed prepare just means the later load pays the imports.
                logger.error("prewarm failed: %s", e, exc_info=True)
                _send_error(proto_out, req_id, str(e), traceback.format_exc())

        elif mtype == "configure":
            if instance is not None:
                _send_error(
                    proto_out,
                    req_id,
                    f"already configured (as {inference_id}); configure is "
                    "allowed exactly once per worker",
                )
                continue
            requested_id = msg.get("inference_id", "<unknown>")
            config = msg.get("config") or {}
            try:
                instance = impl_cls(**config)
            except Exception as e:
                # Per-request: a failed instantiation leaves the worker
                # un-configured but alive (configure may be retried).
                instance = None
                logger.error(
                    "%s - configure failed: %s", requested_id, e, exc_info=True
                )
                _send_error(proto_out, req_id, str(e), traceback.format_exc())
                continue
            inference_id = requested_id
            logger.info("Configured as %s", inference_id)
            _send_ok(proto_out, req_id)

        elif mtype == "load":
            if instance is None:
                _send_error(
                    proto_out,
                    req_id,
                    "load before configure",
                )
                continue
            before: dict = {}
            try:
                # Bracket the load for the footprint the orchestrator charges.
                before = memory.begin_load()
                # Idempotency lives in the impl's own load() guard
                # (InferenceModel implementations early-return when loaded).
                instance.load()
                # A pin that named nothing is a silent CPU fallback.
                pin_problem = memory.pinned_device_missing()
                if pin_problem is not None:
                    raise RuntimeError(pin_problem)
                loaded = True
                report = memory.finish_load(before, instance)
                # The per-item pixel canvas only this process can see: a
                # ceiling in a downloaded processor config (protocol doc).
                from inferio_worker import packing

                canvas_pixels = packing.impl_canvas_pixels(instance)
                if canvas_pixels is not None:
                    report["canvas_pixels"] = canvas_pixels
                _send_ok(proto_out, req_id, **report)
            except Exception as e:
                # The other half of the bracket: a raised load leaves
                # `finish_load` unreached and its context probe polling.
                memory.abort_load(before)
                logger.error(
                    "%s - load failed: %s", inference_id, e, exc_info=True
                )
                _send_error(proto_out, req_id, str(e), traceback.format_exc())

        elif mtype == "predict":
            if instance is None:
                _send_error(
                    proto_out,
                    req_id,
                    "predict before configure",
                )
                continue
            if not loaded:
                _send_error(
                    proto_out,
                    req_id,
                    "predict before a successful load",
                )
                continue
            grant = msg.get("grant")
            if not isinstance(grant, dict):
                grant = None
            if grant is not None:
                # Admissibility gate: an impl that batches inside `predict`
                # reports a size the peaks do not describe, so it takes the
                # grantless path (protocol doc, "Memory grants").
                from inferio_worker import packing

                if packing.batching_disabled(instance):
                    if not batching_off_logged:
                        batching_off_logged = True
                        logger.info(
                            "%s - this impl has its own batching disabled; "
                            "ignoring memory grants and running each window in "
                            "one predict call (no calibration units reported)",
                            inference_id,
                        )
                    grant = None
            try:
                inputs = [
                    prediction_input_from_frame(entry)
                    for entry in msg.get("inputs") or []
                ]
                if grant is None:
                    # Compatibility path: the whole window is one GPU batch,
                    # as before the harness existed.
                    batch = memory.begin_batch()
                    outputs = list(instance.predict(inputs))
                    _send_ok(
                        proto_out,
                        req_id,
                        outputs=outputs,
                        **memory.finish_batch(batch, items=len(inputs)),
                    )
                else:
                    # Granted: the harness prices, packs, clamps, measures.
                    from inferio_worker import packing

                    _send_ok(
                        proto_out,
                        req_id,
                        **packing.run_window(
                            instance,
                            inputs,
                            grant,
                            _memory_frame_emitter(
                                proto_out, req_id, batch_memory_frames
                            ),
                        ),
                    )
            except Exception as e:
                # Includes serialization failures from write_frame (bad
                # output type, oversized response): packing happens before
                # any byte hits the stream, so we can still reply with a
                # clean error frame and keep serving.
                logger.error(
                    "%s - predict failed: %s", inference_id, e, exc_info=True
                )
                extra: dict[str, Any] = {}
                measurements = getattr(e, "measurements", None)
                if measurements:
                    # A partial window still measured what ran.
                    extra["measurements"] = measurements
                    sample = memory.device_memory_sample()
                    if sample is not None:
                        extra["memory"] = sample
                _send_error(
                    proto_out, req_id, str(e), traceback.format_exc(), **extra
                )

        elif mtype == "unload":
            # Valid in every state: a parked prewarmed worker with no
            # instance is dismissed the same way (ok + exit 0).
            try:
                if loaded and instance is not None:
                    instance.unload()
            except Exception as e:
                # Stay alive per the error semantics; the orchestrator's
                # terminate/kill ladder handles a worker that cannot unload.
                logger.error(
                    "%s - unload failed: %s", inference_id, e, exc_info=True
                )
                _send_error(proto_out, req_id, str(e), traceback.format_exc())
                continue
            _send_ok(proto_out, req_id)
            proto_out.flush()
            logger.info("Unloaded; exiting.")
            return EXIT_OK

        elif mtype == "trim":
            # Orchestrator-initiated pool release: not unload, only the
            # allocator's unused blocks, and never an error (protocol doc,
            # "Reactive shrink and trim").
            from inferio_worker import packing

            released = memory.empty_cache()
            if released:
                # The pool regrows from here, so the comparator's rate and
                # the shrink hysteresis are stale. Only when it actually ran.
                packing.note_trimmed()
                logger.info("%s - released the allocator pool on request", inference_id)
            trim_payload: dict[str, Any] = {}
            sample = memory.device_memory_sample()
            if sample is not None:
                # After the release, so `reserved_mb` is what to charge now.
                trim_payload["memory"] = sample
            _send_ok(proto_out, req_id, **trim_payload)

        elif mtype == "ping":
            _send_ok(proto_out, req_id)

        else:
            _send_error(
                proto_out, req_id, f"unsupported request type: {mtype!r}"
            )


def main() -> int:
    proto_in, proto_out = _setup_stdio()
    logging.basicConfig(
        stream=sys.stderr,
        level=os.getenv("INFERIO_WORKER_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        return _serve(proto_in, proto_out)
    except Exception as e:
        # ProtocolError (oversized/garbled frames, EOF mid-frame) and any
        # other unrecoverable failure: log and exit non-zero; the
        # orchestrator surfaces the stderr tail.
        from inferio_worker.protocol import ProtocolError

        if isinstance(e, ProtocolError):
            logger.error("Fatal protocol error: %s", e)
            return EXIT_PROTOCOL_ERROR
        logger.error("Fatal worker error: %s", e, exc_info=True)
        return EXIT_INTERNAL_ERROR


if __name__ == "__main__":
    sys.exit(main())
