#!/usr/bin/env python3
"""Faithful transcription of the outro detector's Python reference
(docs/video-outro-detection-design.md §3.1 + §3.3), for equivalence testing
against the Rust implementation in panoptikon/src/media_tools/outro.rs (§12).

This file is the *specification side* of the comparison. It is deliberately a
straight numpy/subprocess transcription of the design's code block, not a
port of the Rust code: a bug reproduced identically on both sides would be
invisible, which is the whole reason the reference is written from the design
rather than from the implementation.

Two stages, per §3:

  stage 1 (§3.1)  decode the final frame (`-sseof -0.35`), squash it to a
                  fixed 32x32 ignoring aspect ratio, and test its frame
                  median against the card colour. A *rejector only*.
  stage 2 (§3.3)  for what stage 1 promotes: decode the last 7s at 30fps
                  scaled to 48px wide, score every frame, find the terminal
                  gap-tolerant run, apply R0-R3, and report K = run.

Two traps are handled explicitly, both named in the design:

* **`scale=48:-2` height rounding (§3.4).** ffmpeg rounds the derived height
  half-*up* to a multiple of two. Python's `round()` is banker's rounding and
  computes 68 where ffmpeg produces 70 (576x828 -> 828*48/576 = 69.0), the
  rawvideo buffer then fails to reshape and a perfectly good file is
  misclassified as a probe error. The formula used here is
  `int(h * 48 / w / 2 + 0.5) * 2`.

* **Rotation.** The reference needs the frame height *before* it can slice
  the raw stream, so unlike the Rust side (which reads ffmpeg's own reported
  output geometry off stderr, and only falls back to stored dims) it must
  call ffprobe. ffprobe reports **coded** dimensions, while ffmpeg's filter
  graph auto-rotates its input — so on any rotated phone capture the coded
  w/h and the filtered w/h are swapped relative to each other. The rotation
  is therefore read from the stream's side data (or the legacy `rotate` tag)
  and w/h swapped when |rotation| is 90 or 270, so both sides compute the
  same scaled height. Getting this wrong shows up as a reshape failure, not
  as a wrong verdict — but it would be a *reference* failure attributed to
  the implementation.

Median semantics (§12's other named divergence risk): `np.median` over an
even-length axis averages the two middle values. `h * 48` is always even
here (h is forced even by `scale=48:-2`), so the averaging branch is the
only one that ever runs, and the Rust `median_u8` must match it.

Used as a library by run_equivalence.py; runnable on its own for one file:

    python tools/outro-equivalence/reference.py PATH [PATH ...]
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, asdict
from typing import Optional

import numpy as np

# --- algorithm constants (design §3.3) --------------------------------------
W, FPS, TAIL_S = 48, 30, 7
CARD_BG = np.array([12, 13, 25])
TOL, BGFRAC_TOL, BGFRAC_MIN = 8, 12, 0.45
RUN_MEAN_MIN = 0.90
MIN_RUN_S, MIN_LEAD_S, K_CAP = 1.0, 0.40, 5.0
INK_DELTA, INK_ROWS_MAX = 25, 0.60

# --- stage 1 constants (design §3.1) ----------------------------------------
GATE_SIZE = 32
GATE_SSEOF = "-0.35"

FFMPEG = os.environ.get("OUTRO_EQUIV_FFMPEG") or "ffmpeg"
FFPROBE = os.environ.get("OUTRO_EQUIV_FFPROBE") or "ffprobe"

# Windows: keep ffmpeg/ffprobe consoles from flashing when the runner fans out.
_NO_WINDOW = {"creationflags": 0x08000000} if sys.platform == "win32" else {}


@dataclass
class Result:
    path: str
    status: str  # "ok" | "error"
    verdict: Optional[str] = None  # "tiktok_card" | "none"
    reason: Optional[str] = None  # gate | no-run | no-boundary | too-long | layout
    k: Optional[float] = None
    gate: Optional[bool] = None
    error: Optional[str] = None
    # Coarse failure class, comparable with the Rust side's OutroProbeError
    # variants: "spawn" (ffmpeg never ran — never a verdict on the media) vs
    # "decode" (it ran and failed — ambiguous between a broken file and a
    # transient mount hiccup). The two map to different ledger outcomes
    # (§7.2), so a spawn-vs-decode pairing is a real divergence, not
    # agreement, and the comparison must see it. Emitted as `class`, which
    # cannot be a Python identifier.
    error_class: Optional[str] = None
    ms: Optional[int] = None

    def as_json(self) -> str:
        record = asdict(self)
        record["class"] = record.pop("error_class")
        return json.dumps(record)


class ProbeError(Exception):
    """ffmpeg/ffprobe ran and could not produce a usable decode."""

    kind = "decode"


class SpawnError(ProbeError):
    """ffmpeg/ffprobe could not be launched at all."""

    kind = "spawn"


def _run(args: list[str]) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **_NO_WINDOW,
        )
    except OSError as err:
        # Never a verdict on the media: the toolchain is missing or unusable.
        raise SpawnError(f"{args[0]} failed to start: {err}") from err


def scaled_height(width: int, height: int) -> Optional[int]:
    """The height ffmpeg's `scale=48:-2` derives for a width x height source.

    Half-up to a multiple of two, exactly as ffmpeg does it (§3.4). `None`
    for the degenerate ratios that round away to nothing, where swscale falls
    back to something this cannot reproduce.
    """
    if width <= 0 or height <= 0:
        return None
    scaled = int(height * W / width / 2 + 0.5) * 2
    return scaled if scaled >= 2 else None


def display_dims(path: str) -> tuple[int, int]:
    """The video stream's dimensions *as the filter graph sees them*: coded
    dimensions with the container/stream rotation applied.
    """
    proc = _run(
        [
            FFPROBE,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_streams",
            "-of",
            "json",
            path,
        ]
    )
    if proc.returncode != 0:
        raise ProbeError(
            "ffprobe exited with %d: %s"
            % (proc.returncode, proc.stderr.decode("utf-8", "replace").strip()[-400:])
        )
    try:
        streams = json.loads(proc.stdout.decode("utf-8", "replace")).get("streams") or []
    except json.JSONDecodeError as err:
        raise ProbeError(f"ffprobe emitted no parsable JSON: {err}") from err
    if not streams:
        raise ProbeError("no video stream")
    stream = streams[0]
    width, height = stream.get("width"), stream.get("height")
    if not width or not height:
        raise ProbeError("video stream has no dimensions")

    rotation = 0.0
    for side_data in stream.get("side_data_list") or []:
        if "rotation" in side_data:
            rotation = float(side_data["rotation"])
            break
    else:
        tag = (stream.get("tags") or {}).get("rotate")
        if tag is not None:
            try:
                rotation = float(tag)
            except ValueError:
                rotation = 0.0
    if round(abs(rotation)) % 180 == 90:
        width, height = height, width
    return int(width), int(height)


# --- stage 1 (design §3.1) --------------------------------------------------


def gate_promotes(path: str) -> bool:
    """Decode the last frame, squash to 32x32, test the frame median.

    A source under ~3fps can legitimately have no frame inside the last
    0.35s; ffmpeg then exits cleanly having emitted nothing, which is not a
    probe error — just nothing to promote.
    """
    proc = _run(
        [
            FFMPEG,
            "-nostdin",
            "-hide_banner",
            "-nostats",
            "-v",
            "error",
            "-sseof",
            GATE_SSEOF,
            "-i",
            path,
            "-vf",
            f"scale={GATE_SIZE}:{GATE_SIZE},format=rgb24",
            "-f",
            "rawvideo",
            "-",
        ]
    )
    if proc.returncode != 0:
        raise ProbeError(
            "ffmpeg exited with %d: %s"
            % (proc.returncode, proc.stderr.decode("utf-8", "replace").strip()[-400:])
        )
    frame_len = GATE_SIZE * GATE_SIZE * 3
    raw = proc.stdout
    if len(raw) % frame_len != 0:
        raise ProbeError(
            f"gate produced a partial frame ({len(raw) % frame_len} trailing bytes)"
        )
    if not raw:
        return False
    last = np.frombuffer(raw[-frame_len:], dtype=np.uint8).astype(np.int16)
    med = np.median(last.reshape(GATE_SIZE * GATE_SIZE, 3), 0)
    return bool(np.abs(med - CARD_BG).max() <= TOL)


# --- stage 2 (design §3.3) --------------------------------------------------


def scan_tail(path: str) -> Result:
    width, height = display_dims(path)
    h = scaled_height(width, height)
    if h is None:
        raise ProbeError(f"{width}x{height} scales away to nothing")

    proc = _run(
        [
            FFMPEG,
            "-nostdin",
            "-hide_banner",
            "-nostats",
            "-v",
            "error",
            "-sseof",
            f"-{TAIL_S}",
            "-i",
            path,
            "-vf",
            f"fps={FPS},scale={W}:-2,format=rgb24",
            "-f",
            "rawvideo",
            "-",
        ]
    )
    if proc.returncode != 0:
        raise ProbeError(
            "ffmpeg exited with %d: %s"
            % (proc.returncode, proc.stderr.decode("utf-8", "replace").strip()[-400:])
        )
    raw = proc.stdout
    frame_len = h * W * 3
    if len(raw) % frame_len != 0:
        raise ProbeError(
            f"ffmpeg produced a partial frame ({len(raw) % frame_len} trailing "
            f"bytes at {W}x{h}, {len(raw)} received)"
        )
    n = len(raw) // frame_len
    if n == 0:
        raise ProbeError("ffmpeg produced no frames")

    # §3.4: `-sseof` is not guaranteed, so nothing here may assume <= 210
    # frames. K is anchored to the end of the stream, which is what makes an
    # ignored tail seek harmless.
    a = np.frombuffer(raw, dtype=np.uint8).reshape(n, h, W, 3).astype(np.int16)

    # --- design §3.3, verbatim ---
    flat = a.reshape(n, h * W, 3)
    med = np.median(flat, 1)
    on_bg = np.abs(med - CARD_BG).max(1) <= TOL
    near = (np.abs(flat - med[:, None, :]).max(2) <= BGFRAC_TOL).mean(1)
    card = on_bg & (near >= BGFRAC_MIN)

    i = n
    for j in range(n - 1, -1, -1):
        if card[j] and card[j:].mean() >= RUN_MEAN_MIN:
            i = j
    run, lead = (n - i) / FPS, i / FPS

    if run < MIN_RUN_S:
        return Result(path=path, status="ok", verdict="none", reason="no-run", gate=True)
    if lead < MIN_LEAD_S:
        return Result(
            path=path, status="ok", verdict="none", reason="no-boundary", gate=True
        )
    if run > K_CAP:
        return Result(
            path=path, status="ok", verdict="none", reason="too-long", gate=True
        )
    ink = np.abs(a[-1] - CARD_BG).max(2) > INK_DELTA
    if float(ink.any(1).mean()) > INK_ROWS_MAX:
        return Result(path=path, status="ok", verdict="none", reason="layout", gate=True)
    return Result(path=path, status="ok", verdict="tiktok_card", k=float(run), gate=True)


def detect_outro(path: str) -> Result:
    """Both stages, in order. The gate is a rejector only."""
    import time

    started = time.perf_counter()
    try:
        if not gate_promotes(path):
            result = Result(
                path=path, status="ok", verdict="none", reason="gate", gate=False
            )
        else:
            result = scan_tail(path)
    except ProbeError as err:
        result = Result(
            path=path, status="error", error=str(err), error_class=err.kind
        )
    result.ms = int((time.perf_counter() - started) * 1000)
    return result


if __name__ == "__main__":
    for argument in sys.argv[1:]:
        print(detect_outro(argument).as_json())
