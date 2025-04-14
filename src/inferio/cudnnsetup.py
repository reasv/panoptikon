import os
import sys
import platform
import shutil
from pathlib import Path
import tempfile

try:
    import requests
except ImportError:
    requests = None

import tarfile
import zipfile
import logging

logger = logging.getLogger(__name__)

CUDNN_URLS = {
    "linux_amd64": "https://developer.download.nvidia.com/compute/cudnn/redist/cudnn/linux-x86_64/cudnn-linux-x86_64-9.8.0.87_cuda12-archive.tar.xz",
    "windows_amd64": "https://developer.download.nvidia.com/compute/cudnn/redist/cudnn/windows-x86_64/cudnn-windows-x86_64-9.8.0.87_cuda12-archive.zip",
}


def platform_key():
    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "linux" and ("x86_64" in machine or "amd64" in machine):
        return "linux_amd64"
    elif system == "windows" and ("x86_64" in machine or "amd64" in machine):
        return "windows_amd64"
    return None


def download_file(url, dest_path):
    if requests is None:
        raise RuntimeError("requests library is required to download cuDNN")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def extract_zip_cudnn(src_zip, dst_folder):
    with zipfile.ZipFile(src_zip, "r") as z:
        archive_root = None
        # Find archive root directory
        for name in z.namelist():
            parts = name.split('/')
            if len(parts) > 1 and parts[1] in ("bin", "lib", "include"):
                archive_root = parts[0]
                break
        if archive_root is None:
            raise RuntimeError("Could not find cuDNN archive structure in zip")
        # Extract bin, lib, include into dst_folder
        for name in z.namelist():
            if name.startswith(archive_root + "/bin") or name.startswith(archive_root + "/lib") or name.startswith(archive_root + "/include"):
                dest_path = dst_folder / '/'.join(name.split('/')[1:])  # skip archive root
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                if not name.endswith('/'):
                    with z.open(name) as src, open(dest_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)


def extract_tar_cudnn(src_tar, dst_folder):
    """
    Extracts all from tarfile to temp folder, then copies just lib and include folders
    into dst_folder. Cleans up after itself.
    """
    dst_folder = Path(dst_folder)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)  # ensure pathlib
        logger.info(f"Extracting entire archive to tempdir: {tempdir}")
        with tarfile.open(src_tar, "r:*") as tar:
            tar.extractall(tempdir)
        logger.info("Extraction complete.")
        # Find the first-level extracted directory (archive usually contains one root folder)
        extracted_dirs = [p for p in tempdir.iterdir() if p.is_dir()]
        if not extracted_dirs:
            raise RuntimeError("No directory found in cuDNN archive after extraction!")
        archive_root = extracted_dirs[0]  # usually only one, per Nvidia's style
        # Find lib and include folders under that root (could be deeper, so generalize)
        for subdir in ("lib", "include"):
            found = list(archive_root.glob(f"**/{subdir}"))  # search recursively
            for src in found:
                dst = dst_folder / subdir
                if dst.exists():
                    shutil.rmtree(dst)
                logger.info(f"Copying {src} -> {dst}")
                shutil.copytree(src, dst)
        logger.info("Copy complete.")
    # Optionally delete the original archive
    os.remove(src_tar)

def ensure_cudnn():
    project_root = Path(__file__).resolve().parent.parent.parent
    cudnn_home = project_root / "cudnn"

    # On Windows, require bin AND lib; on Linux, require lib.
    key = platform_key()
    ready = False
    if key == "windows_amd64":
        ready = (cudnn_home / "bin").exists() and (cudnn_home / "lib").exists()
    elif key == "linux_amd64":
        ready = (cudnn_home / "lib").exists()
    else:
        ready = (cudnn_home).exists()

    if ready:
        return True

    if not key:
        # Check if macos
        if platform.system().lower() != "darwin":
            logger.info("cuDNN download not supported on this platform. Make sure you manually place cuDNN in the 'cudnn' folder.")
        return False

    url = CUDNN_URLS[key]
    archive_name = url.split("/")[-1]
    archive_path = project_root / archive_name
    logger.info(f"Downloading cuDNN from {url} ...")
    try:
        download_file(url, archive_path)
    except Exception as e:
        logger.error("Download failed:", e)
        return False

    try:
        logger.info(f"Extracting {archive_name} ...")
        if key == "windows_amd64":
            extract_zip_cudnn(str(archive_path), cudnn_home)
        elif key == "linux_amd64":
            extract_tar_cudnn(str(archive_path), cudnn_home)
        logger.info("Extraction complete.")
    except Exception as e:
        logger.error("Extraction failed:", e)
        return False
    finally:
        archive_path.unlink(missing_ok=True)

    return True


def add_cudnn_to_path():
    project_root = Path(__file__).resolve().parent.parent.parent
    cudnn_path = project_root / "cudnn"

    env_sep = os.pathsep

    # Windows: prefer bin for DLLs
    # Linux: lib contains *.so, no bin
    if platform.system().lower() == "windows":
        cudnn_bin_path = cudnn_path / "bin"
        os.environ["PATH"] = str(cudnn_bin_path) + env_sep + os.environ.get("PATH", "")
    else:
        cudnn_lib_path = cudnn_path / "lib"
        os.environ["LD_LIBRARY_PATH"] = str(cudnn_lib_path) + env_sep + os.environ.get("LD_LIBRARY_PATH", "")
        # Also add to PATH as fallback for unusual setups
        os.environ["PATH"] = str(cudnn_lib_path) + env_sep + os.environ.get("PATH", "")

    os.environ["CUDA_PATH"] = str(cudnn_path)


def cudnn_setup():
    """
    Ensure cuDNN is installed and set up correctly.
    Downloads and extracts cuDNN if not already present.
    Adds cuDNN to PATH and LD_LIBRARY_PATH (Linux).
    """
    if ensure_cudnn():
        add_cudnn_to_path()
    else:
        logger.error("cuDNN setup could not be completed. Please install it manually.")