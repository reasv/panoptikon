import os
import platform
import logging
from pathlib import Path
import importlib

logger = logging.getLogger(__name__)

def _project_root() -> Path:
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
    """
    Add directory to Windows DLL search path (Python 3.8+).
    Falls back to PATH prepend if add_dll_directory is unavailable.
    """
    if not p or not p.exists():
        return
    try:
        add = getattr(os, "add_dll_directory", None)
        if add is not None:
            add(str(p))
        else:
            _prepend_env("PATH", str(p))
    except Exception as e:
        # Don't hard-fail; PATH prepend as last resort
        logger.debug("os.add_dll_directory failed for %s: %s", p, e)
        _prepend_env("PATH", str(p))

def _nvidia_component_dirs(component: str) -> list[Path]:
    """
    Locate runtime dirs for NVIDIA pip components.
    component examples: "nvidia.cudnn", "nvidia.cublas", "nvidia.cuda_runtime"
    """
    mod = _try_import(component)
    if mod is None or not getattr(mod, "__file__", None):
        return []
    # .../site-packages/nvidia/<component>
    base = Path(mod.__file__).resolve().parent # type: ignore  
    candidates = []

    # Typical layouts: bin/ for Windows DLLs, lib/ for .so/.dylib
    for sub in ("bin", "lib", "lib64"):
        p = base / sub
        if p.exists():
            candidates.append(p)

    # Some NVIDIA wheels use nested paths; be conservative and only include
    # known common subdirs to avoid huge directory scans.
    return candidates

def _legacy_cudnn_dirs() -> list[Path]:
    """
    Your old layout: project_root/cudnn/{bin,lib}
    """
    cudnn_path = _project_root() / "cudnn"
    candidates = []
    if (cudnn_path / "bin").exists():
        candidates.append(cudnn_path / "bin")
    if (cudnn_path / "lib").exists():
        candidates.append(cudnn_path / "lib")
    if cudnn_path.exists():
        # keep root as very last resort (sometimes DLLs get dropped here)
        candidates.append(cudnn_path)
    return candidates

def _torch_lib_dir() -> Path | None:
    """
    Torch often ships CUDA runtime DLLs in site-packages/torch/lib on Windows.
    Adding this helps with cublasLt64_12.dll-style errors when torch is installed.
    """
    torch = _try_import("torch")
    if torch is None or not getattr(torch, "__file__", None):
        return None
    p = Path(torch.__file__).resolve().parent / "lib" # type: ignore
    return p if p.exists() else None

def ensure_cudnn() -> bool:
    """
    Returns True if we can find cuDNN either via NVIDIA pip wheels or legacy folder.
    """
    # Preferred: NVIDIA wheels
    cudnn_dirs = _nvidia_component_dirs("nvidia.cudnn")
    if cudnn_dirs:
        return True

    # Fallback: legacy vendored folder
    if _legacy_cudnn_dirs():
        return True

    return False

def add_cudnn_to_path() -> None:
    """
    Register CUDA-related runtime directories for the current process.

    - Windows: uses os.add_dll_directory for deterministic DLL loading.
    - Linux: prepends LD_LIBRARY_PATH for the current process.
    """
    system = platform.system().lower()

    # Preferred dirs from NVIDIA wheels
    cudnn_dirs = _nvidia_component_dirs("nvidia.cudnn")

    # cuBLAS is separate; if installed via pip, register it too.
    cublas_dirs = _nvidia_component_dirs("nvidia.cublas")

    # Optional: CUDA runtime component wheel(s), if you choose to install them.
    cuda_runtime_dirs = _nvidia_component_dirs("nvidia.cuda_runtime")

    # Torch's own lib dir (helps with cublasLt64_12.dll in many setups)
    torch_lib = _torch_lib_dir()
    torch_dirs = [torch_lib] if torch_lib else []

    # Legacy fallback
    legacy_dirs = _legacy_cudnn_dirs()

    # Build a de-duplicated list, preserving precedence:
    # 1) NVIDIA wheels (cudnn/cublas/runtime)
    # 2) torch/lib
    # 3) legacy folder
    dirs: list[Path] = []
    for group in (cudnn_dirs, cublas_dirs, cuda_runtime_dirs, torch_dirs, legacy_dirs):
        for p in group:
            if p and p.exists() and p not in dirs:
                dirs.append(p)

    if not dirs:
        logger.warning("No CUDA runtime directories found to add to loader path.")
        return

    if system == "windows":
        for p in dirs:
            _win_add_dll_dir(p)
        # PATH still matters for some subprocesses; keep it consistent
        for p in dirs:
            _prepend_env("PATH", str(p))
    else:
        # Linux: loader checks LD_LIBRARY_PATH at process start; setting it here
        # can still help if your app loads libs after this call.
        for p in dirs:
            _prepend_env("LD_LIBRARY_PATH", str(p))
        for p in dirs:
            _prepend_env("PATH", str(p))

    # CUDA_PATH is not really "cuDNN home"; keep it only for legacy compatibility.
    # Prefer not to set it when using pip wheels, but leaving it won’t usually hurt.
    legacy_root = _project_root() / "cudnn"
    if legacy_root.exists():
        os.environ["CUDA_PATH"] = str(legacy_root)

def cudnn_setup() -> None:
    """
    Ensure cuDNN is available and register library search paths.

    This no longer downloads cuDNN.
    """
    if os.getenv("NO_CUDNN", "false").lower() in ("1", "true"):
        logger.info("cuDNN setup is disabled by environment variable NO_CUDNN=true")
        return

    if not ensure_cudnn():
        logger.error(
            "cuDNN not found. Install it in the venv (recommended: nvidia-cudnn-cu12) "
            "or provide a legacy ./cudnn folder."
        )
        return

    add_cudnn_to_path()
