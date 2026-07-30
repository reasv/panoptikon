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
    last_oom_retry,
    run_with_oom_retry,
    total_oom_halvings,
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


def test_the_call_records_what_it_actually_executed():
    """The worker's packing harness reads this to answer 'did the impl run the
    batch it was given?'. A batch it only partly ran is unpriceable, and the
    halving count is a negative sample the orchestrator would otherwise never
    hear about (docs/inferio-worker-protocol.md, "Memory sensing")."""
    calls = []

    def process(chunk):
        calls.append(len(chunk))
        if len(chunk) > 2:
            raise FakeOOM("too big")
        return list(chunk)

    before = last_oom_retry()
    result, _ = _retry(process, list(range(8)))
    assert result == list(range(8))
    record = last_oom_retry()
    assert record is not None
    generation, largest, halvings = record
    assert largest == 2, "8 -> 4 -> 2 before anything ran"
    assert halvings == 2
    if before is not None:
        assert generation > before[0], "each call gets a fresh generation"

    # A whole-batch run records the whole batch and no halvings.
    _retry(lambda chunk: list(chunk), list(range(5)))
    assert last_oom_retry()[1:] == (5, 0)

    # An initial_chunk_size below the batch is the florence2/dots_ocr shape:
    # the impl sub-batched with no OOM at all, which is still unpriceable.
    _retry(lambda chunk: list(chunk), list(range(6)), initial_chunk_size=2)
    assert last_oom_retry()[1:] == (2, 0)


def test_the_record_is_reset_per_call():
    """A call that runs nothing must not leave the previous call's numbers
    standing — that is exactly how a stale record would mis-price a batch."""
    _retry(lambda chunk: list(chunk), list(range(4)))
    assert last_oom_retry()[1] == 4
    _retry(lambda chunk: list(chunk), [])
    assert last_oom_retry()[1] == 0, "an empty call executed nothing"


def test_the_process_total_halvings_counter_accumulates_across_calls():
    """The per-call record keeps the *last* call only, and several impls call
    this helper twice per `predict` (clip and nemotron-embed-vl run a text pass
    and an image pass). The worker harness diffs this monotonic total across the
    whole `predict` call so a halving in any of them is still reported."""

    def halving_once(chunk):
        if len(chunk) > 1:
            raise FakeOOM("too big")
        return list(chunk)

    before = total_oom_halvings()
    _retry(halving_once, list(range(2)))
    after_first = total_oom_halvings()
    assert after_first == before + 1
    # A later clean call leaves the total where it was — and leaves a record
    # that says nothing was halved, which is the case this counter covers.
    _retry(lambda chunk: list(chunk), list(range(2)))
    assert last_oom_retry()[2] == 0, "the record forgot the earlier halving"
    assert total_oom_halvings() == after_first, "the total did not"


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
