"""Unit tests for the worker's loader-path setup (`inferio_worker.cudnn`).

The Linux half of this module used to *prepend* `LD_LIBRARY_PATH` from inside
the already-running worker, which `ld.so` cannot act on — it read the
variable when the process started. The visible cost was `faster_whisper`
(CTranslate2, no RPATH) aborting the worker on load with "Unable to load any
of {libcudnn_ops.so.9…}" while every torch impl was fine. The host now puts
those directories in the spawn environment, and this module reports when they
are missing instead of pretending to fix it.

These run everywhere: the platform and every directory source are faked, so
no CUDA wheel, GPU or Linux host is needed.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from inferio_worker import cudnn


@pytest.fixture
def fake_wheels(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[Path]:
    """A Linux worker whose venv ships cuDNN and cuBLAS and nothing else."""
    dirs = {}
    for component in ("nvidia.cudnn", "nvidia.cublas"):
        lib = tmp_path / component.replace(".", "/") / "lib"
        lib.mkdir(parents=True)
        dirs[component] = [lib]

    monkeypatch.setattr(cudnn.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        cudnn, "_nvidia_component_dirs", lambda component: dirs.get(component, [])
    )
    monkeypatch.setattr(cudnn, "_torch_lib_dir", lambda: None)
    monkeypatch.setattr(cudnn, "_legacy_cudnn_dirs", list)
    monkeypatch.setattr(cudnn, "_project_root", lambda: tmp_path / "no-such-root")
    monkeypatch.setattr(cudnn, "_warned_about_loader_path", False)
    monkeypatch.delenv("LD_LIBRARY_PATH", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin")
    return [dirs["nvidia.cudnn"][0], dirs["nvidia.cublas"][0]]


def test_the_linux_branch_does_not_touch_ld_library_path(
    fake_wheels: list[Path],
) -> None:
    """The defect itself: an in-process assignment the loader never reads.

    Setting it here is worse than useless — it is what made the gap look
    closed. The only place it works is the environment the host spawns the
    worker with.
    """
    cudnn.cudnn_setup()
    assert "LD_LIBRARY_PATH" not in os.environ


def test_the_child_process_path_is_still_prepended(fake_wheels: list[Path]) -> None:
    """PATH is inherited by children, so prepending it does buy something."""
    cudnn.cudnn_setup()
    entries = os.environ["PATH"].split(os.pathsep)
    assert set(str(p) for p in fake_wheels) <= set(entries)
    assert entries[-1] == "/usr/bin"


def test_a_missing_loader_path_is_reported_once_and_names_the_dirs(
    fake_wheels: list[Path], caplog: pytest.LogCaptureFixture
) -> None:
    """The state in which CTranslate2 aborts: say so, name the value."""
    with caplog.at_level(logging.WARNING, logger=cudnn.logger.name):
        cudnn.cudnn_setup()
        cudnn.cudnn_setup()

    warnings = [r for r in caplog.records if "LD_LIBRARY_PATH" in r.getMessage()]
    assert len(warnings) == 1, "one message per process, not one per call"
    message = warnings[0].getMessage()
    for lib in fake_wheels:
        assert str(lib) in message
    assert "faster_whisper" in message


def test_nothing_is_reported_when_the_host_set_the_loader_path(
    fake_wheels: list[Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The fixed configuration, including a symlinked path to the same dir.

    The host canonicalizes what it injects (a venv's `lib64` is usually a
    symlink to `lib`), so the comparison has to be on the real path rather
    than the string.
    """
    real, aliased = fake_wheels
    link = aliased.parent.parent / "alias"
    link.symlink_to(aliased.parent, target_is_directory=True)
    monkeypatch.setenv(
        "LD_LIBRARY_PATH", os.pathsep.join([str(real), str(link / "lib")])
    )

    with caplog.at_level(logging.WARNING, logger=cudnn.logger.name):
        cudnn.cudnn_setup()

    assert not [r for r in caplog.records if "LD_LIBRARY_PATH" in r.getMessage()]


def test_no_wheels_means_no_loader_path_complaint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A CPU venv ships no NVIDIA wheels; there is nothing to be missing."""
    monkeypatch.setattr(cudnn.platform, "system", lambda: "Linux")
    monkeypatch.setattr(cudnn, "_nvidia_component_dirs", lambda component: [])
    monkeypatch.setattr(cudnn, "_torch_lib_dir", lambda: tmp_path)
    monkeypatch.setattr(cudnn, "_legacy_cudnn_dirs", list)
    monkeypatch.setattr(cudnn, "_project_root", lambda: tmp_path / "no-such-root")
    monkeypatch.setattr(cudnn, "_warned_about_loader_path", False)
    monkeypatch.delenv("LD_LIBRARY_PATH", raising=False)

    with caplog.at_level(logging.WARNING, logger=cudnn.logger.name):
        cudnn.cudnn_setup()

    assert not [r for r in caplog.records if "LD_LIBRARY_PATH" in r.getMessage()]
    assert "LD_LIBRARY_PATH" not in os.environ
