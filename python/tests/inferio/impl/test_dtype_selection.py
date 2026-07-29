"""Unit tests for the dtype negotiation helpers in inferio.impl.utils.

These run everywhere (no GPU needed): device capabilities are mocked, and
the CT2 probe uses an injected fake ctranslate2 module. The real-hardware
paths (Pascal CT2 rejection, sm_75 bf16 downgrade) are unverifiable here
and are covered only by these mocks — see docs/gpu-compatibility-design.md.
"""

import logging
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

import torch

from inferio.impl.utils import (
    cuda_capability,
    get_device,
    select_ct2_compute_type,
    select_dtype,
)


CUDA = torch.device("cuda")
CPU = torch.device("cpu")
MPS = torch.device("mps")


def _patch_cap(major: int, minor: int):
    return mock.patch.object(
        torch.cuda, "get_device_capability", return_value=(major, minor)
    )


def _patch_hip(value):
    return mock.patch.object(torch.version, "hip", value)


class TestCudaCapability:
    def test_cpu_and_mps_are_none(self):
        assert cuda_capability(CPU) is None
        assert cuda_capability(MPS) is None

    def test_cuda_reports_capability(self):
        with _patch_hip(None), _patch_cap(7, 5):
            assert cuda_capability(CUDA) == (7, 5)

    def test_hip_is_none_even_on_cuda_device(self):
        with _patch_hip("6.2"):
            assert cuda_capability(CUDA) is None


class TestSelectDtype:
    def test_bf16_on_ampere(self):
        with _patch_hip(None), _patch_cap(8, 0):
            assert select_dtype(CUDA, "bf16") is torch.bfloat16

    @pytest.mark.parametrize("cap", [(7, 5), (6, 1)])
    def test_bf16_below_ampere_steps_to_fp32_not_fp16(self, cap):
        with _patch_hip(None), _patch_cap(*cap):
            assert select_dtype(CUDA, "bf16") is torch.float32

    def test_fp16_on_every_cuda_arch(self):
        with _patch_hip(None), _patch_cap(6, 1):
            assert select_dtype(CUDA, "fp16") is torch.float16

    def test_fp32_passthrough(self):
        with _patch_hip(None), _patch_cap(8, 0):
            assert select_dtype(CUDA, "fp32") is torch.float32

    @pytest.mark.parametrize("device", [CPU, MPS])
    def test_non_cuda_is_fp32(self, device):
        assert select_dtype(device, "bf16") is torch.float32
        assert select_dtype(device, "fp16") is torch.float32

    def test_explicit_wins_over_negotiation(self):
        with _patch_hip(None), _patch_cap(6, 1):
            assert (
                select_dtype(CUDA, "bf16", explicit="fp16")
                is torch.float16
            )

    def test_explicit_bf16_below_ampere_warns_but_wins(self, caplog):
        logger = logging.getLogger("test-dtype")
        with _patch_hip(None), _patch_cap(6, 1):
            with caplog.at_level(logging.WARNING, logger="test-dtype"):
                got = select_dtype(
                    CUDA, "fp16", explicit="bf16", logger=logger
                )
        assert got is torch.bfloat16
        assert any("below" in rec.message for rec in caplog.records)

    def test_long_aliases(self):
        with _patch_hip(None), _patch_cap(8, 0):
            assert select_dtype(CUDA, "bfloat16") is torch.bfloat16
            assert select_dtype(CUDA, "float16") is torch.float16
        assert select_dtype(CPU, "float32") is torch.float32

    def test_unknown_name_raises(self):
        with pytest.raises(ValueError, match="Unknown precision"):
            select_dtype(CPU, "int8")

    def test_rocm_bf16_uses_torch_probe(self):
        with _patch_hip("6.2"):
            with mock.patch.object(
                torch.cuda, "is_bf16_supported", return_value=True
            ):
                assert select_dtype(CUDA, "bf16") is torch.bfloat16
            with mock.patch.object(
                torch.cuda, "is_bf16_supported", return_value=False
            ):
                assert select_dtype(CUDA, "bf16") is torch.float32


class TestGetDeviceCapabilityGuard:
    """get_device drops CUDA devices whose capability has no kernels in
    the build's arch list (same major, minor >= arch minor)."""

    def _patch_cuda(self, *, count, archs, caps_by_index, hip=None):
        def capability(device):
            index = device.index if device.index is not None else 0
            return caps_by_index[index]

        return (
            mock.patch.object(torch.cuda, "is_available", return_value=True),
            mock.patch.object(torch.cuda, "device_count", return_value=count),
            mock.patch.object(torch.cuda, "get_arch_list", return_value=archs),
            mock.patch.object(
                torch.cuda, "get_device_capability", side_effect=capability
            ),
            _patch_hip(hip),
        )

    def _get_device(self, **kwargs):
        patches = self._patch_cuda(**kwargs)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            return get_device()

    def test_covered_device_kept(self):
        devices = self._get_device(
            count=1, archs=["sm_50", "sm_80", "sm_120"], caps_by_index={0: (12, 0)}
        )
        assert devices == [torch.device("cuda")]

    def test_forward_compat_within_major(self):
        # An sm_60 cubin runs on an sm_61 device.
        devices = self._get_device(
            count=1, archs=["sm_60"], caps_by_index={0: (6, 1)}
        )
        assert devices == [torch.device("cuda")]

    def test_uncovered_device_dropped(self, caplog):
        with caplog.at_level(logging.WARNING, logger="inferio.impl.utils"):
            devices = self._get_device(
                count=2,
                archs=["sm_75", "sm_80"],
                caps_by_index={0: (6, 1), 1: (8, 6)},
            )
        assert devices == [torch.device("cuda:1")]
        assert any("Dropping GPU" in rec.message for rec in caplog.records)

    def test_all_dropped_falls_back_to_cpu(self, caplog):
        with caplog.at_level(logging.ERROR, logger="inferio.impl.utils"):
            devices = self._get_device(
                count=1, archs=["sm_80"], caps_by_index={0: (6, 1)}
            )
        assert devices == [torch.device("cpu")]
        assert any(
            "falling back to CPU" in rec.message for rec in caplog.records
        )

    def test_hip_skips_filtering(self):
        devices = self._get_device(
            count=1,
            archs=["gfx1100"],
            caps_by_index={0: (11, 0)},
            hip="6.2",
        )
        assert devices == [torch.device("cuda")]

    def test_empty_arch_list_skips_filtering(self):
        devices = self._get_device(
            count=1, archs=[], caps_by_index={0: (6, 1)}
        )
        assert devices == [torch.device("cuda")]


class _FakeCT2(SimpleNamespace):
    pass


def _patch_ct2(supported_by_kind):
    """Inject a fake ctranslate2 whose get_supported_compute_types either
    returns the set for the queried kind or raises the stored exception."""

    def get_supported_compute_types(kind):
        result = supported_by_kind[kind]
        if isinstance(result, Exception):
            raise result
        return result

    fake = _FakeCT2(get_supported_compute_types=get_supported_compute_types)
    return mock.patch.dict(sys.modules, {"ctranslate2": fake})


def _patch_cuda_available(value: bool):
    return mock.patch.object(torch.cuda, "is_available", return_value=value)


class TestSelectCt2ComputeType:
    def test_float16_when_supported(self):
        with _patch_ct2({"cuda": {"float16", "float32", "int8"}}):
            with _patch_cuda_available(True):
                assert select_ct2_compute_type() == "float16"

    def test_falls_back_to_float32_when_float16_unsupported(self):
        with _patch_ct2({"cuda": {"float32", "int8"}}):
            with _patch_cuda_available(True):
                assert select_ct2_compute_type() == "float32"

    def test_preferred_other_than_float16(self):
        with _patch_ct2({"cuda": {"int8_float16", "float16", "float32"}}):
            with _patch_cuda_available(True):
                assert (
                    select_ct2_compute_type("int8_float16")
                    == "int8_float16"
                )

    def test_cpu_kind_when_cuda_unavailable(self):
        with _patch_ct2({"cpu": {"float32", "int8"}}):
            with _patch_cuda_available(False):
                assert select_ct2_compute_type() == "float32"

    def test_probe_failure_falls_back_to_float32(self):
        with _patch_ct2({"cuda": RuntimeError("no CUDA backend")}):
            with _patch_cuda_available(True):
                assert select_ct2_compute_type() == "float32"

    def test_explicit_wins_without_probing(self):
        # No fake module injected: explicit must return before any import.
        with mock.patch.dict(sys.modules, {"ctranslate2": None}):
            assert select_ct2_compute_type(explicit="int8") == "int8"
