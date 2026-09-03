#!/usr/bin/env python3
"""corpus.py - deterministic media corpus with a per-item unit-cost manifest.

Ground truth for what the packer should have priced
(`docs/batch-calibration-test-protocol.md` §2). Everything is generated from a
seed, so the same command on any platform produces byte-identical inputs and an
identical manifest; the manifest carries, per item, the units each of the
feature's four cost dimensions would charge it
(`python/inferio_worker/packing.py: price_inputs`).

Usage
-----
    corpus.py --tier smoke --out results/corpus/smoke
    corpus.py --tier ramp  --out results/corpus/ramp [--seed 20260903] [--jobs 8]
    corpus.py --list-tiers
    corpus.py --tier ramp --out /tmp/x --dry-run     # plan + size estimate only

Options:
    --tier NAME       one of the tiers below (required unless --list-tiers)
    --out DIR         destination directory (created; must be empty or --force)
    --seed N          master seed                              (default: 20260903)
    --scale F         multiply every group's count by F         (default: 1.0)
    --jobs N          worker processes                          (default: cpu/2)
    --force           write into a non-empty directory
    --manifest PATH   manifest location (default: <out>/manifest.json)
    --dry-run         print the plan without generating anything

Tiers
-----
    smoke   ~200 items, a little of everything (images incl. RGBA PNG and a
            few 8000x6000 JPEGs, text, audio, PDFs).
    ramp    ~2000 uniform 1024x1024 JPEGs -- the S2 cold-ramp corpus.
    text    ~2000 text files, 40 B .. 8 kB, incl. CJK (token pricing).
    pixmix  ~600 images at 0.3 / 1 / 4 / 20 MP (S8 pixel/sum).
    ocr     ~400 images from 256px thumbnails to 8000x6000 scans
            (S8 pixel/max-times-count, the easyOCR acceptance test).
    audio   ~200 WAV/MP3 clips of 5 .. 120 s.
    pdf     ~120 PDFs of 1 .. 40 pages.
    soak    ~12000 items: ramp + pixmix + text + audio + pdf, mixed.
    poison  deliberate failure inputs for S5: a truncated JPEG, a zero-byte
            file, a file literally named `out of memory.png`, and one very
            large PNG (see --poison-side; the default already costs ~1 GB of
            RAM to encode).

Output
------
`<out>/manifest.json`:

    {"schema": "corpus/1", "tier": str, "seed": int, "root": "<abs out dir>",
     "generated_at": "<ISO-8601 UTC>", "scale": float,
     "counts": {"image": int, "text": int, "audio": int, "pdf": int, ...},
     "total_bytes": int, "elapsed_s": float,
     "items": [
       {"id": str, "path": "<relative to root>", "abspath": str,
        "kind": "image"|"text"|"audio"|"pdf"|"junk",
        "group": "<tier group label>",
        "format": "JPEG"|"PNG"|"WEBP"|"WAV"|"MP3"|"PDF"|"TXT"|null,
        "mime": str, "bytes": int,
        "width": int|null, "height": int|null, "pixels": int|null,
        "seconds": float|null, "pages": int|null,
        "text_bytes": int|null, "script": "latin"|"cjk"|null,
        "units": {"item": 1, "pixel": int|null, "token": int|null,
                  "audio-second": int|null}}
     ]}

`units.pixel` is `width*height` of the *submitted* file, which is what
`price_inputs` charges (raw dimensions, finding W1). `units.token` is
`max(1, utf8_bytes // 4)`. `units["audio-second"]` is the flat 30 the harness
charges every audio item, not the real duration (`seconds` holds that).
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import math
import os
import random
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BYTES_PER_TOKEN = 4  # packing.py BYTES_PER_TOKEN
AUDIO_FLAT_UNITS = 30  # packing.py: audio-second is a flat 30 per item

MIME = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "WEBP": "image/webp",
    "WAV": "audio/wav",
    "MP3": "audio/mpeg",
    "PDF": "application/pdf",
    "TXT": "text/plain",
}


# --------------------------------------------------------------------------
# Plan
# --------------------------------------------------------------------------


@dataclass
class Group:
    """One homogeneous block of items in a tier."""

    label: str
    kind: str
    count: int
    params: Dict[str, Any] = field(default_factory=dict)


def tier_groups(tier: str) -> List[Group]:
    if tier == "smoke":
        return [
            Group("img-1024-jpg", "image", 100, {"w": 1024, "h": 1024, "format": "JPEG"}),
            Group("img-512-jpg", "image", 30, {"w": 512, "h": 512, "format": "JPEG"}),
            Group("img-2048x1152-jpg", "image", 20,
                  {"w": 2048, "h": 1152, "format": "JPEG"}),
            Group("img-rgba-png", "image", 20,
                  {"w": 768, "h": 768, "format": "PNG", "alpha": True}),
            Group("img-webp", "image", 5, {"w": 640, "h": 480, "format": "WEBP"}),
            Group("img-8000x6000-jpg", "image", 5,
                  {"w": 8000, "h": 6000, "format": "JPEG"}),
            Group("txt-short", "text", 10, {"bytes": 160}),
            Group("txt-long", "text", 5, {"bytes": 8000}),
            Group("wav-10s", "audio", 3, {"seconds": 10.0, "format": "WAV"}),
            Group("mp3-30s", "audio", 2, {"seconds": 30.0, "format": "MP3"}),
            Group("pdf-4p", "pdf", 5, {"pages": 4, "w": 1240, "h": 1754}),
        ]
    if tier == "ramp":
        # S2 wants 2000 uniform 1024^2 images: one cost per item, no variance.
        return [Group("img-1024-jpg", "image", 2000,
                      {"w": 1024, "h": 1024, "format": "JPEG"})]
    if tier == "text":
        return [
            Group("txt-40b", "text", 400, {"bytes": 40}),
            Group("txt-256b", "text", 500, {"bytes": 256}),
            Group("txt-1k", "text", 500, {"bytes": 1024}),
            Group("txt-4k", "text", 400, {"bytes": 4096}),
            Group("txt-8k", "text", 150, {"bytes": 8192}),
            Group("txt-cjk-1k", "text", 50, {"bytes": 1024, "script": "cjk"}),
        ]
    if tier == "pixmix":
        return [
            Group("img-0.3mp", "image", 200, {"w": 640, "h": 480, "format": "JPEG"}),
            Group("img-1mp", "image", 200, {"w": 1024, "h": 1024, "format": "JPEG"}),
            Group("img-4mp", "image", 150, {"w": 2048, "h": 2048, "format": "JPEG"}),
            Group("img-20mp", "image", 50, {"w": 5200, "h": 3900, "format": "JPEG"}),
        ]
    if tier == "ocr":
        return [
            Group("scan-256", "image", 150, {"w": 256, "h": 256, "format": "JPEG",
                                             "text_page": True}),
            Group("scan-1240x1754", "image", 150,
                  {"w": 1240, "h": 1754, "format": "JPEG", "text_page": True}),
            Group("scan-2480x3508", "image", 80,
                  {"w": 2480, "h": 3508, "format": "JPEG", "text_page": True}),
            Group("scan-8000x6000", "image", 20,
                  {"w": 8000, "h": 6000, "format": "JPEG", "text_page": True}),
        ]
    if tier == "audio":
        return [
            Group("wav-5s", "audio", 80, {"seconds": 5.0, "format": "WAV"}),
            Group("wav-30s", "audio", 60, {"seconds": 30.0, "format": "WAV"}),
            Group("mp3-30s", "audio", 40, {"seconds": 30.0, "format": "MP3"}),
            Group("mp3-120s", "audio", 20, {"seconds": 120.0, "format": "MP3"}),
        ]
    if tier == "pdf":
        return [
            Group("pdf-1p", "pdf", 40, {"pages": 1, "w": 1240, "h": 1754}),
            Group("pdf-8p", "pdf", 50, {"pages": 8, "w": 1240, "h": 1754}),
            Group("pdf-40p", "pdf", 30, {"pages": 40, "w": 1240, "h": 1754}),
        ]
    if tier == "soak":
        groups: List[Group] = [
            Group("img-1024-jpg", "image", 6000,
                  {"w": 1024, "h": 1024, "format": "JPEG"}),
            Group("img-512-jpg", "image", 2000, {"w": 512, "h": 512, "format": "JPEG"}),
            Group("img-2048x1152-jpg", "image", 800,
                  {"w": 2048, "h": 1152, "format": "JPEG"}),
            Group("img-rgba-png", "image", 400,
                  {"w": 768, "h": 768, "format": "PNG", "alpha": True}),
            Group("img-4mp", "image", 400, {"w": 2048, "h": 2048, "format": "JPEG"}),
            Group("img-20mp", "image", 60, {"w": 5200, "h": 3900, "format": "JPEG"}),
            Group("img-8000x6000-jpg", "image", 20,
                  {"w": 8000, "h": 6000, "format": "JPEG"}),
        ]
        groups.extend(tier_groups("text"))
        groups.extend(tier_groups("audio"))
        groups.extend(tier_groups("pdf"))
        return groups
    if tier == "poison":
        return [
            Group("truncated-jpg", "junk", 1, {"style": "truncated"}),
            Group("empty", "junk", 1, {"style": "empty"}),
            Group("named-oom", "junk", 1, {"style": "named_oom"}),
            Group("huge-png", "image", 1,
                  {"w": 16000, "h": 16000, "format": "PNG", "huge": True}),
        ]
    raise SystemExit(f"corpus: unknown tier {tier!r}")


TIERS = ("smoke", "ramp", "text", "pixmix", "ocr", "audio", "pdf", "soak", "poison")


# --------------------------------------------------------------------------
# Generators (run in worker processes; must be importable at module level)
# --------------------------------------------------------------------------


def _rng(seed: int, index: int) -> random.Random:
    return random.Random((seed * 1_000_003) ^ (index * 2_654_435_761))


def _base_image(width: int, height: int, rnd: random.Random, alpha: bool,
                text_page: bool):
    """A deterministic, JPEG-friendly synthetic image.

    Smooth gradients plus a handful of shapes: realistic entropy for the
    encoder without the multi-megabyte files pure noise would produce.
    """
    import numpy as np
    from PIL import Image, ImageDraw

    if text_page:
        # A "scanned page": light ground with dark ruled text lines. Cheap to
        # encode and it exercises OCR-ish preprocessing paths.
        image = Image.new("RGB", (width, height), (238, 236, 230))
        draw = ImageDraw.Draw(image)
        margin = max(4, width // 16)
        line_h = max(2, height // 60)
        y = margin
        while y < height - margin:
            run = rnd.randint(int(width * 0.3), int(width * 0.92))
            shade = rnd.randint(20, 70)
            draw.rectangle(
                [margin, y, min(width - margin, margin + run), y + line_h],
                fill=(shade, shade, shade),
            )
            y += line_h * 3
        return image

    # Two orthogonal gradients plus a low-amplitude ripple, per-item phased.
    # Above ~2 MP the gradient is computed small and resized up: it is smooth
    # by construction, and a full-resolution float32 meshgrid for a 48 MP scan
    # would cost ~1 GB of RAM per worker process.
    GRADIENT_MAX = 1024
    gw = min(width, GRADIENT_MAX)
    gh = max(1, min(height, int(round(height * gw / max(1, width)))))
    xs = np.linspace(0.0, 1.0, gw, dtype=np.float32)
    ys = np.linspace(0.0, 1.0, gh, dtype=np.float32)
    grid_x, grid_y = np.meshgrid(xs, ys)
    phase = rnd.random() * math.tau
    freq = 2.0 + rnd.random() * 6.0
    red = (grid_x * 255.0)
    green = (grid_y * 255.0)
    blue = (
        127.0
        + 100.0 * np.sin(freq * (grid_x + grid_y) * math.pi + phase)
    )
    stack = np.stack([red, green, blue], axis=-1)
    stack = np.clip(stack, 0.0, 255.0).astype(np.uint8)
    image = Image.fromarray(stack, "RGB")
    if (gw, gh) != (width, height):
        image = image.resize((width, height), Image.BILINEAR)

    draw = ImageDraw.Draw(image)
    for _ in range(6):
        x0 = rnd.randint(0, max(0, width - 2))
        y0 = rnd.randint(0, max(0, height - 2))
        x1 = min(width - 1, x0 + rnd.randint(width // 16 + 1, max(2, width // 3)))
        y1 = min(height - 1, y0 + rnd.randint(height // 16 + 1, max(2, height // 3)))
        colour = (rnd.randint(0, 255), rnd.randint(0, 255), rnd.randint(0, 255))
        if rnd.random() < 0.5:
            draw.rectangle([x0, y0, x1, y1], fill=colour)
        else:
            draw.ellipse([x0, y0, x1, y1], fill=colour)

    if alpha:
        alpha_band = Image.new("L", (width, height), 255)
        adraw = ImageDraw.Draw(alpha_band)
        adraw.ellipse(
            [width // 8, height // 8, width - width // 8, height - height // 8],
            fill=rnd.randint(40, 200),
        )
        image = image.convert("RGBA")
        image.putalpha(alpha_band)
    return image


def gen_image(spec: Dict[str, Any]) -> Dict[str, Any]:
    from PIL import Image

    Image.MAX_IMAGE_PIXELS = None  # we deliberately create bomb-sized inputs
    path = Path(spec["abspath"])
    params = spec["params"]
    width, height = int(params["w"]), int(params["h"])
    fmt = params.get("format", "JPEG")
    rnd = _rng(spec["seed"], spec["index"])
    image = _base_image(width, height, rnd, bool(params.get("alpha")),
                        bool(params.get("text_page")))
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "JPEG":
        if image.mode != "RGB":
            image = image.convert("RGB")
        image.save(path, "JPEG", quality=int(params.get("quality", 85)),
                   optimize=False)
    elif fmt == "PNG":
        image.save(path, "PNG", compress_level=int(params.get("compress", 1)))
    elif fmt == "WEBP":
        image.save(path, "WEBP", quality=int(params.get("quality", 85)))
    else:
        raise SystemExit(f"corpus: unsupported image format {fmt!r}")
    return {
        "format": fmt,
        "mime": MIME[fmt],
        "width": width,
        "height": height,
        "pixels": width * height,
        "bytes": path.stat().st_size,
        "units": {"item": 1, "pixel": width * height, "token": None,
                  "audio-second": None},
    }


# A deterministic word pool: the text has to be stable across platforms, so it
# is built from a fixed vocabulary rather than from any system dictionary.
_WORDS = (
    "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi "
    "omicron pi rho sigma tau upsilon phi chi psi omega vector tensor batch "
    "memory ledger grant window anchor slope residual calibration headroom "
    "throughput deflation ramp probe oracle corpus manifest inference worker"
).split()
_CJK = "日本語文字列埋め込み検索対象文書処理速度計測基準値検証用資料"


def gen_text(spec: Dict[str, Any]) -> Dict[str, Any]:
    path = Path(spec["abspath"])
    params = spec["params"]
    want = int(params["bytes"])
    script = params.get("script", "latin")
    rnd = _rng(spec["seed"], spec["index"])
    chunks: List[str] = []
    size = 0
    while size < want:
        piece = (
            "".join(rnd.choice(_CJK) for _ in range(rnd.randint(6, 18)))
            if script == "cjk"
            else " ".join(rnd.choice(_WORDS) for _ in range(rnd.randint(4, 12)))
        )
        piece += ". "
        chunks.append(piece)
        size += len(piece.encode("utf-8"))
    blob = "".join(chunks).encode("utf-8")[:want]
    # Never split a UTF-8 sequence: back off to the last clean boundary.
    while blob:
        try:
            blob.decode("utf-8")
            break
        except UnicodeDecodeError:
            blob = blob[:-1]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)
    nbytes = path.stat().st_size
    return {
        "format": "TXT",
        "mime": MIME["TXT"],
        "bytes": nbytes,
        "text_bytes": nbytes,
        "script": script,
        "units": {"item": 1, "pixel": None,
                  "token": max(1, nbytes // BYTES_PER_TOKEN),
                  "audio-second": None},
    }


def gen_audio(spec: Dict[str, Any]) -> Dict[str, Any]:
    path = Path(spec["abspath"])
    params = spec["params"]
    seconds = float(params["seconds"])
    fmt = params.get("format", "WAV")
    rnd = _rng(spec["seed"], spec["index"])
    freq = 180 + rnd.randint(0, 900)
    beat = 0.5 + rnd.random() * 3.0
    path.parent.mkdir(parents=True, exist_ok=True)
    ffmpeg = spec.get("ffmpeg") or "ffmpeg"
    cmd = [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi",
        "-i", f"sine=frequency={freq}:beep_factor={beat:.3f}:duration={seconds}",
        "-ar", "16000", "-ac", "1",
    ]
    cmd += ["-c:a", "libmp3lame", "-b:a", "64k"] if fmt == "MP3" else ["-c:a", "pcm_s16le"]
    cmd.append(str(path))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 or not path.exists():
        raise RuntimeError(
            f"ffmpeg failed for {path.name}: {result.stderr.strip()[:400]}"
        )
    return {
        "format": fmt,
        "mime": MIME[fmt],
        "bytes": path.stat().st_size,
        "seconds": seconds,
        "units": {"item": 1, "pixel": None, "token": None,
                  "audio-second": AUDIO_FLAT_UNITS},
    }


def gen_pdf(spec: Dict[str, Any]) -> Dict[str, Any]:
    from PIL import Image

    path = Path(spec["abspath"])
    params = spec["params"]
    pages = int(params["pages"])
    width, height = int(params["w"]), int(params["h"])
    images = [
        _base_image(width, height, _rng(spec["seed"], spec["index"] * 977 + page),
                    False, True).convert("RGB")
        for page in range(pages)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    # Pillow stamps /CreationDate and /ModDate from the wall clock, which
    # would make every regeneration a different file. Pin them to the epoch so
    # the corpus is byte-reproducible.
    epoch = time.gmtime(0)
    try:
        images[0].save(path, "PDF", resolution=150.0, save_all=True,
                       append_images=images[1:], creationDate=epoch,
                       modDate=epoch)
    except TypeError:  # older Pillow without the date kwargs
        images[0].save(path, "PDF", resolution=150.0, save_all=True,
                       append_images=images[1:])
    return {
        "format": "PDF",
        "mime": MIME["PDF"],
        "bytes": path.stat().st_size,
        "pages": pages,
        "width": width,
        "height": height,
        "pixels": width * height,
        "units": {"item": 1, "pixel": width * height * pages, "token": None,
                  "audio-second": None},
    }


def gen_junk(spec: Dict[str, Any]) -> Dict[str, Any]:
    from PIL import Image

    path = Path(spec["abspath"])
    style = spec["params"]["style"]
    path.parent.mkdir(parents=True, exist_ok=True)
    if style == "empty":
        path.write_bytes(b"")
    elif style == "truncated":
        buffer = Path(str(path) + ".full")
        image = _base_image(512, 512, _rng(spec["seed"], spec["index"]), False, False)
        image.save(buffer, "JPEG", quality=85)
        blob = buffer.read_bytes()
        buffer.unlink()
        path.write_bytes(blob[: len(blob) // 2])
    elif style == "named_oom":
        image = _base_image(256, 256, _rng(spec["seed"], spec["index"]), False, False)
        image.save(path, "PNG")
    else:
        raise SystemExit(f"corpus: unknown junk style {style!r}")
    return {
        "format": None,
        "mime": "application/octet-stream",
        "bytes": path.stat().st_size,
        "units": {"item": 1, "pixel": None, "token": None, "audio-second": None},
    }


GENERATORS = {
    "image": gen_image,
    "text": gen_text,
    "audio": gen_audio,
    "pdf": gen_pdf,
    "junk": gen_junk,
}


def generate_one(spec: Dict[str, Any]) -> Dict[str, Any]:
    try:
        produced = GENERATORS[spec["kind"]](spec)
    except Exception as exc:  # one bad item must not lose the whole corpus
        return {**spec, "error": f"{type(exc).__name__}: {exc}"}
    item = {
        "id": spec["id"],
        "path": spec["relpath"],
        "abspath": spec["abspath"],
        "kind": spec["kind"],
        "group": spec["group"],
        "format": None,
        "mime": None,
        "bytes": 0,
        "width": None,
        "height": None,
        "pixels": None,
        "seconds": None,
        "pages": None,
        "text_bytes": None,
        "script": None,
        "units": {},
    }
    item.update(produced)
    return item


# --------------------------------------------------------------------------
# Planning and driving
# --------------------------------------------------------------------------


EXT = {"JPEG": "jpg", "PNG": "png", "WEBP": "webp", "WAV": "wav", "MP3": "mp3",
       "PDF": "pdf", "TXT": "txt"}


def plan(tier: str, out: Path, seed: int, scale: float,
         ffmpeg: Optional[str], poison_side: Optional[int]) -> List[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    index = 0
    for group in tier_groups(tier):
        count = max(1, int(round(group.count * scale))) if group.count else 0
        params = dict(group.params)
        if poison_side and params.get("huge"):
            params["w"] = params["h"] = poison_side
        for ordinal in range(count):
            index += 1
            if group.kind == "junk":
                style = params["style"]
                name = {
                    "empty": "empty.bin",
                    "truncated": "truncated.jpg",
                    "named_oom": "out of memory.png",
                }[style]
                relpath = f"{group.kind}/{name}"
            else:
                fmt = params.get("format", "TXT" if group.kind == "text" else "PDF")
                relpath = (
                    f"{group.kind}/{group.label}/"
                    f"{group.label}-{ordinal:06d}.{EXT[fmt]}"
                )
            specs.append(
                {
                    "id": f"{group.label}-{ordinal:06d}",
                    "group": group.label,
                    "kind": group.kind,
                    "params": params,
                    "seed": seed,
                    "index": index,
                    "relpath": relpath,
                    "abspath": str(out / relpath),
                    "ffmpeg": ffmpeg,
                }
            )
    return specs


def estimate_bytes(specs: List[Dict[str, Any]]) -> int:
    """Rough size estimate for --dry-run (measured rates, +/- a factor of 2)."""
    total = 0
    for spec in specs:
        params = spec["params"]
        if spec["kind"] == "image":
            pixels = int(params.get("w", 0)) * int(params.get("h", 0))
            fmt = params.get("format", "JPEG")
            per_px = {"JPEG": 0.12, "PNG": 0.9, "WEBP": 0.1}.get(fmt, 0.2)
            total += int(pixels * per_px)
        elif spec["kind"] == "text":
            total += int(params.get("bytes", 0))
        elif spec["kind"] == "audio":
            rate = 8000 if params.get("format") == "MP3" else 32000
            total += int(float(params.get("seconds", 0)) * rate)
        elif spec["kind"] == "pdf":
            pixels = int(params.get("w", 0)) * int(params.get("h", 0))
            total += int(pixels * 0.12 * int(params.get("pages", 1)))
        else:
            total += 4096
    return total


def human(size: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size} B"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Deterministic media corpus generator with a unit manifest.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--tier", choices=TIERS)
    parser.add_argument("--out", help="destination directory")
    parser.add_argument("--seed", type=int, default=20260903)
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 4) // 2))
    parser.add_argument("--force", action="store_true",
                        help="allow writing into a non-empty directory")
    parser.add_argument("--manifest", help="manifest path (default: <out>/manifest.json)")
    parser.add_argument("--ffmpeg", default=shutil.which("ffmpeg") or "ffmpeg")
    parser.add_argument("--poison-side", type=int, default=None,
                        help="override the `poison` tier's huge-PNG side length")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--list-tiers", action="store_true")
    args = parser.parse_args(argv)

    if args.list_tiers:
        for tier in TIERS:
            groups = tier_groups(tier)
            total = sum(group.count for group in groups)
            print(f"{tier:8s} {total:6d} items  "
                  f"{human(estimate_bytes(plan(tier, Path('/tmp'), 0, 1.0, None, None)))}")
            for group in groups:
                print(f"           {group.count:6d}  {group.label:22s} {group.params}")
        return 0

    if not args.tier or not args.out:
        parser.error("--tier and --out are required (or use --list-tiers)")

    out = Path(args.out).resolve()
    specs = plan(args.tier, out, args.seed, args.scale, args.ffmpeg, args.poison_side)

    if args.dry_run:
        print(f"tier {args.tier}: {len(specs)} items, "
              f"estimated {human(estimate_bytes(specs))} into {out}")
        by_group: Dict[str, int] = {}
        for spec in specs:
            by_group[spec["group"]] = by_group.get(spec["group"], 0) + 1
        for label, count in by_group.items():
            print(f"  {count:6d}  {label}")
        return 0

    if out.exists() and any(out.iterdir()) and not args.force:
        raise SystemExit(f"corpus: {out} is not empty (use --force)")
    out.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    items: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    if args.jobs <= 1:
        results = [generate_one(spec) for spec in specs]
    else:
        with futures.ProcessPoolExecutor(max_workers=args.jobs) as pool:
            results = list(pool.map(generate_one, specs, chunksize=8))
    for result in results:
        (errors if result.get("error") else items).append(result)
        if len(items) % 500 == 0 and items:
            print(f"  {len(items)}/{len(specs)}", file=sys.stderr)

    elapsed = time.monotonic() - started
    counts: Dict[str, int] = {}
    for item in items:
        counts[item["kind"]] = counts.get(item["kind"], 0) + 1
    manifest = {
        "schema": "corpus/1",
        "tier": args.tier,
        "seed": args.seed,
        "scale": args.scale,
        "root": str(out),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "total_bytes": sum(item["bytes"] for item in items),
        "elapsed_s": round(elapsed, 3),
        "errors": errors,
        "items": items,
    }
    manifest_path = Path(args.manifest) if args.manifest else out / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    print(
        f"corpus {args.tier}: {len(items)} items, {human(manifest['total_bytes'])}, "
        f"{elapsed:.1f}s -> {manifest_path}"
        + (f"  ({len(errors)} FAILED)" if errors else "")
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
