"""Unit tests for `inferio.impl.utils.get_device` and its `INFERIO_DEVICE`
override (docs/unified-memory-admission.md, backend C, "Device coherence").

The override is what makes "priced against system RAM" and "runs on the CPU"
one decision instead of two that have to agree: `get_device` probes the
*machine*, while the orchestrator prices what the installed wheels and the
configuration say. On a box with an NVIDIA card and the CPU wheels — or an
`accelerator = "cpu"` Mac — those disagree, and the model would otherwise run
somewhere nothing budgeted a batch against.
"""

import logging
import os
from unittest import mock

import torch

from inferio.impl.utils import DEVICE_ENV_VAR, forced_device, get_device


def test_no_marker_leaves_the_probe_alone():
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop(DEVICE_ENV_VAR, None)
        assert forced_device() is None
        # Whatever this machine has, the probe is what answered — the value
        # is not asserted, only that nothing forced it.
        assert get_device()

    # An empty value is "not configured", not "no device".
    with mock.patch.dict(os.environ, {DEVICE_ENV_VAR: "  "}, clear=False):
        assert forced_device() is None


def test_the_marker_wins_over_an_available_accelerator():
    # The whole point: this test box has CUDA available, and the model must
    # still land on the CPU because that is what it is priced against.
    with mock.patch.dict(os.environ, {DEVICE_ENV_VAR: "cpu"}, clear=False):
        assert forced_device() == "cpu"
        assert get_device() == [torch.device("cpu")]
    # Case and surrounding whitespace are not a different device.
    with mock.patch.dict(os.environ, {DEVICE_ENV_VAR: " CPU "}, clear=False):
        assert get_device() == [torch.device("cpu")]


def test_an_unknown_device_warns_and_falls_back_to_probing(caplog):
    # A newer orchestrator naming a device this worker does not know must
    # degrade to probing rather than fail every load — and must say so, or
    # the mismatch is invisible.
    with mock.patch.dict(os.environ, {DEVICE_ENV_VAR: "xpu"}, clear=False):
        with caplog.at_level(logging.WARNING, logger="inferio.impl.utils"):
            assert forced_device() is None
            forced = get_device()
    assert "xpu" in caplog.text, caplog.text
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop(DEVICE_ENV_VAR, None)
        assert forced == get_device(), "the probe decided, as if unset"
