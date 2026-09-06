"""CUDA/cuDNN loader-path setup for worker processes.

Duplicated (trimmed) from `inferio.cudnnsetup` so the harness never imports
the `inferio` package. Registers runtime library directories for the current
process before the impl class is instantiated:

- Windows: `os.add_dll_directory` (PATH prepend as fallback / for children).
- Linux: prepends PATH for child processes, and *checks* the loader path.

On Linux the search path of a later `dlopen` cannot be changed from inside
the process: `ld.so` reads `LD_LIBRARY_PATH` once, when the process starts,
and assigning to `os.environ` afterwards does nothing for it. So the host
puts the venv's `nvidia/*/lib` in the worker's **spawn** environment
(`accelerator_env::worker_env`, docs/inferio-worker-protocol.md) and this
module only reports, once, when they are missing — the state in which an impl
on a library without RPATH (CTranslate2, i.e. `faster_whisper`) aborts the
worker on load rather than raising. Torch is unaffected either way: it finds
these same files through the RPATH of its own extension modules.

All failures are non-fatal; the worker proceeds with a warning.
"""

from __future__ import annotations

import importlib
import logging
import os
import platform
from pathlib import Path

logger = logging.getLogger(__name__)


def _project_root() -> Path:
    # python/inferio_worker/cudnn.py -> python/inferio_worker -> python -> repo root
    return Path(__file__).resolve().parent.parent.parent


def _try_import(modname: str):
    try:
        return importlib.import_module(modname)
    except Exception:
        return None


def _prepend_env(name: str, value: str) -> None:
    sep = os.pathsep
    cur = os.environ.get(name, "")
    if not cur:
        os.environ[name] = value
        return
    parts = cur.split(sep)
    if value in parts:
        return
    os.environ[name] = value + sep + cur


def _win_add_dll_dir(p: Path) -> None:
    """Add directory to the Windows DLL search path, PATH as last resort."""
    if not p or not p.exists():
        return
    try:
        add = getattr(os, "add_dll_directory", None)
        if add is not None:
            add(str(p))
        else:
            _prepend_env("PATH", str(p))
    except Exception as e:
        logger.debug("os.add_dll_directory failed for %s: %s", p, e)
        _prepend_env("PATH", str(p))


def _nvidia_component_dirs(component: str) -> list[Path]:
    """Locate runtime dirs for NVIDIA pip components (e.g. nvidia.cudnn)."""
    mod = _try_import(component)
    if mod is None or not getattr(mod, "__file__", None):
        return []
    base = Path(mod.__file__).resolve().parent  # type: ignore
    candidates = []
    for sub in ("bin", "lib", "lib64"):
        p = base / sub
        if p.exists():
            candidates.append(p)
    return candidates


def _legacy_cudnn_dirs() -> list[Path]:
    """Legacy vendored layout: project_root/cudnn/{bin,lib}."""
    cudnn_path = _project_root() / "cudnn"
    candidates = []
    if (cudnn_path / "bin").exists():
        candidates.append(cudnn_path / "bin")
    if (cudnn_path / "lib").exists():
        candidates.append(cudnn_path / "lib")
    if cudnn_path.exists():
        candidates.append(cudnn_path)
    return candidates


def _torch_lib_dir() -> Path | None:
    """Torch ships CUDA runtime DLLs in site-packages/torch/lib on Windows."""
    torch = _try_import("torch")
    if torch is None or not getattr(torch, "__file__", None):
        return None
    p = Path(torch.__file__).resolve().parent / "lib"  # type: ignore
    return p if p.exists() else None


_warned_about_loader_path = False


def _warn_if_not_on_loader_path(dirs: list[Path]) -> None:
    """Report once when the host did not put `dirs` on `LD_LIBRARY_PATH`.

    Only the NVIDIA wheel dirs are checked, because those are exactly what
    the host injects; torch's own `lib` and the legacy vendored tree are
    reachable through RPATH and are not expected there. Nothing is set: by
    the time this runs the loader has already read the variable, so the only
    useful action is naming the value the spawn environment should have
    carried. One message per process, at most.
    """
    global _warned_about_loader_path
    if _warned_about_loader_path or not dirs:
        return
    current = {
        os.path.realpath(entry)
        for entry in os.environ.get("LD_LIBRARY_PATH", "").split(os.pathsep)
        if entry
    }
    missing = [p for p in dirs if os.path.realpath(p) not in current]
    if not missing:
        return
    _warned_about_loader_path = True
    logger.warning(
        "LD_LIBRARY_PATH is missing %d NVIDIA runtime director%s this "
        "interpreter ships (%s). The dynamic loader read that variable "
        "before this process started, so it cannot be fixed from here: it "
        "belongs in the environment the worker is spawned with. Impls that "
        "find their CUDA libraries through RPATH (torch, and so most of the "
        "registry) are unaffected; one that does not (CTranslate2, i.e. "
        "faster_whisper) will abort this process on load.",
        len(missing),
        "y" if len(missing) == 1 else "ies",
        os.pathsep.join(str(p) for p in missing),
    )


def _add_cudnn_to_path() -> None:
    system = platform.system().lower()

    cudnn_dirs = _nvidia_component_dirs("nvidia.cudnn")
    cublas_dirs = _nvidia_component_dirs("nvidia.cublas")
    cuda_runtime_dirs = _nvidia_component_dirs("nvidia.cuda_runtime")
    torch_lib = _torch_lib_dir()
    torch_dirs = [torch_lib] if torch_lib else []
    legacy_dirs = _legacy_cudnn_dirs()

    # De-duplicated, precedence: NVIDIA wheels, then torch/lib, then legacy.
    dirs: list[Path] = []
    for group in (
        cudnn_dirs,
        cublas_dirs,
        cuda_runtime_dirs,
        torch_dirs,
        legacy_dirs,
    ):
        for p in group:
            if p and p.exists() and p not in dirs:
                dirs.append(p)

    if not dirs:
        logger.warning(
            "No CUDA runtime directories found to add to loader path."
        )
        return

    if system == "windows":
        for p in dirs:
            _win_add_dll_dir(p)
        # PATH still matters for some subprocesses; keep it consistent.
        for p in dirs:
            _prepend_env("PATH", str(p))
    else:
        # PATH only: it is inherited by children, which is the one thing an
        # in-process assignment can still buy. LD_LIBRARY_PATH is not set
        # here — see the module docstring — it is reported on instead.
        for p in dirs:
            _prepend_env("PATH", str(p))
        if system == "linux":
            _warn_if_not_on_loader_path(
                cudnn_dirs + cublas_dirs + cuda_runtime_dirs
            )

    # Legacy compatibility only; harmless when using pip wheels.
    legacy_root = _project_root() / "cudnn"
    if legacy_root.exists():
        os.environ["CUDA_PATH"] = str(legacy_root)


def cudnn_setup() -> None:
    """Register CUDA library search paths; never raises.

    Honors NO_CUDNN like the legacy `inferio` startup does.
    """
    if os.getenv("NO_CUDNN", "false").lower() in ("1", "true"):
        logger.info("cuDNN setup disabled by NO_CUDNN environment variable")
        return
    try:
        _add_cudnn_to_path()
    except Exception as e:
        logger.warning("cuDNN path setup failed: %s", e, exc_info=True)
