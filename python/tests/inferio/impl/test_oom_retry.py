"""Unit tests for inferio.impl.utils.run_with_oom_retry.

Torch-free where possible: an injected fake exception class stands in for
torch.cuda.OutOfMemoryError, so no GPU (or torch import) is needed.
"""

from pathlib import Path
from unittest import mock

import pytest

from inferio.impl.utils import (
    OOM_BATCH1_PREFIX,
    InferenceOOMError,
    run_with_oom_retry,
)


class FakeOOM(Exception):
    pass


def _retry(process_chunk, items, **kwargs):
    kwargs.setdefault("oom_exceptions", (FakeOOM,))
    with mock.patch("inferio.impl.utils.clear_cache") as cache:
        result = run_with_oom_retry(process_chunk, items, **kwargs)
    return result, cache


def test_no_oom_processes_everything_in_one_chunk():
    calls = []

    def process(chunk):
        calls.append(len(chunk))
        return [x * 2 for x in chunk]

    result, cache = _retry(process, [1, 2, 3, 4])
    assert result == [2, 4, 6, 8]
    assert calls == [4]
    cache.assert_not_called()


def test_halves_on_oom_and_preserves_order():
    calls = []

    def process(chunk):
        calls.append(len(chunk))
        if len(chunk) > 2:
            raise FakeOOM("out of memory")
        return [x * 10 for x in chunk]

    result, cache = _retry(process, list(range(8)))
    assert result == [x * 10 for x in range(8)]
    # 8 fails -> 4 fails -> 2 ok, then the remaining items in chunks of 2.
    assert calls == [8, 4, 2, 2, 2, 2]
    assert cache.call_count == 2


def test_batch1_oom_raises_classified_error():
    def process(chunk):
        raise FakeOOM("CUDA out of memory")

    with mock.patch("inferio.impl.utils.clear_cache") as cache:
        with pytest.raises(InferenceOOMError) as excinfo:
            run_with_oom_retry(process, ["only"], oom_exceptions=(FakeOOM,))
    assert str(excinfo.value).startswith(OOM_BATCH1_PREFIX)
    assert isinstance(excinfo.value.__cause__, FakeOOM)
    # Cleared once per failed attempt, including the final one.
    assert cache.call_count == 1


def test_non_oom_exception_propagates_untouched():
    def process(chunk):
        raise ValueError("bad input")

    with pytest.raises(ValueError, match="bad input"):
        run_with_oom_retry(process, [1, 2], oom_exceptions=(FakeOOM,))


def test_initial_chunk_size_is_respected():
    calls = []

    def process(chunk):
        calls.append(len(chunk))
        return list(chunk)

    result, _ = _retry(process, list(range(7)), initial_chunk_size=3)
    assert result == list(range(7))
    assert calls == [3, 3, 1]


def test_wrong_result_count_raises():
    def process(chunk):
        return chunk[:-1]

    with pytest.raises(RuntimeError, match="returned 1 results for 2"):
        run_with_oom_retry(process, [1, 2], oom_exceptions=(FakeOOM,))


def test_empty_items_returns_empty():
    result, cache = _retry(lambda chunk: chunk, [])
    assert result == []
    cache.assert_not_called()


def test_fixture_prefix_matches_constant():
    """The torch-free worker-protocol fixture hardcodes the prefix; keep
    the literal in lockstep with the real constant."""
    fixture = (
        Path(__file__).resolve().parents[2]
        / "inferio_worker"
        / "fixture_impls"
        / "oom_impl.py"
    )
    assert f'"{OOM_BATCH1_PREFIX}' in fixture.read_text(encoding="utf-8")
