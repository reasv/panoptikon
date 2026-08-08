#!/usr/bin/env python3
"""Stage a pinned PDFium binary and its redistribution notices for Tauri."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath


REPO = Path(__file__).resolve().parent.parent
LOCK_PATH = REPO / "contrib" / "pdfium" / "pdfium-lock.json"
DEFAULT_OUTPUT = REPO / "panoptikon-desktop" / "src-tauri" / "resources" / "pdfium"
DEFAULT_CACHE = REPO / "target" / "pdfium-cache"
LICENSE_NAMES = (
    "Apache-2.0.txt",
    "BSD-3-Clause.txt",
    "LicenseRef-PdfiumThirdParty.txt",
    "dep5-wheel",
)


def host_target() -> str:
    system = platform.system()
    machine = platform.machine().lower()
    if system == "Darwin" and machine in {"arm64", "aarch64"}:
        return "aarch64-apple-darwin"
    if system == "Windows" and machine in {"amd64", "x86_64"}:
        return "x86_64-pc-windows-msvc"
    if system == "Linux" and machine in {"amd64", "x86_64"}:
        return "x86_64-unknown-linux-gnu"
    raise SystemExit(f"unsupported PDFium host: {system} {machine}")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".part", dir=destination.parent
    )
    try:
        with os.fdopen(fd, "wb") as output, urllib.request.urlopen(url) as response:
            shutil.copyfileobj(response, output)
        os.replace(temporary, destination)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def member_with_suffix(names: list[str], suffix: str) -> str:
    matches = [name for name in names if PurePosixPath(name).as_posix().endswith(suffix)]
    if len(matches) != 1:
        raise SystemExit(f"expected one wheel member ending in {suffix!r}, found {matches}")
    return matches[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default=host_target())
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    args = parser.parse_args()

    lock = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    try:
        target = lock["targets"][args.target]
    except KeyError:
        choices = ", ".join(sorted(lock["targets"]))
        raise SystemExit(f"unsupported PDFium target {args.target!r}; choose one of: {choices}")

    url = target["url"]
    expected_hash = target["sha256"]
    wheel = args.cache / PurePosixPath(url).name
    if not wheel.is_file() or sha256(wheel) != expected_hash:
        print(f"Downloading pinned PDFium wheel for {args.target}...")
        download(url, wheel)
    actual_hash = sha256(wheel)
    if actual_hash != expected_hash:
        raise SystemExit(
            f"PDFium wheel hash mismatch: expected {expected_hash}, got {actual_hash}"
        )

    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".pdfium-stage.", dir=output.parent))
    try:
        licenses = temporary / "licenses"
        licenses.mkdir(parents=True)
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            library_member = member_with_suffix(
                names, f"pypdfium2_raw/{target['library']}"
            )
            with archive.open(library_member) as source, (
                temporary / target["library"]
            ).open("wb") as destination:
                shutil.copyfileobj(source, destination)
            version_member = member_with_suffix(names, "pypdfium2_raw/version.json")
            version = json.loads(archive.read(version_member))
            if version.get("build") != lock["pdfium_build"]:
                raise SystemExit(
                    "PDFium build mismatch inside wheel: "
                    f"expected {lock['pdfium_build']}, got {version.get('build')}"
                )
            (temporary / "version.json").write_bytes(archive.read(version_member))
            for license_name in LICENSE_NAMES:
                member = member_with_suffix(names, f".dist-info/{license_name}")
                with archive.open(member) as source, (licenses / license_name).open(
                    "wb"
                ) as destination:
                    shutil.copyfileobj(source, destination)

        manifest = {
            "schema": lock["schema"],
            "target": args.target,
            "pypdfium2_version": lock["pypdfium2_version"],
            "pdfium_build": lock["pdfium_build"],
            "wheel_url": url,
            "wheel_sha256": expected_hash,
            "library": target["library"],
            "library_sha256": sha256(temporary / target["library"]),
        }
        (temporary / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if output.exists():
            shutil.rmtree(output)
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    print(f"Staged PDFium build {lock['pdfium_build']} at {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
