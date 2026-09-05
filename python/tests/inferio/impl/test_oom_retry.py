"""Unit tests for inferio.impl.utils.run_with_oom_retry. Torch-free: an
injected fake exception class stands in for torch.cuda.OutOfMemoryError."""

from pathlib import Path
from unittest import mock

import pytest

from inferio.impl.utils import (
    OOM_BATCH1_PREFIX,
    InferenceOOMError,
    last_oom_retry,
    looks_like_index_limit,
    run_with_oom_retry,
    total_index_limit_events,
    total_oom_halvings,
)


class FakeOOM(Exception):
    pass


def _retry(process_chunk, items, **kwargs):
    kwargs.setdefault("oom_exceptions", (FakeOOM,))
    with mock.patch("inferio.impl.utils.clear_cache") as cache:
        result = run_with_oom_retry(process_chunk, items, **kwargs)
    return result, cache


def _raiser(failure):
    def process(chunk):
        raise failure

    return process


def test_chunking_when_nothing_fails():
    """Whole batch by default, `initial_chunk_size` respected, empty a no-op,
    never a `clear_cache()` — and a short result list is an error."""
    calls = []

    def process(chunk):
        calls.append(len(chunk))
        return [x * 2 for x in chunk]

    for items, kwargs, expected in (
        ([1, 2, 3, 4], {}, [4]),
        (list(range(7)), {"initial_chunk_size": 3}, [3, 3, 1]),
        ([], {}, []),
    ):
        calls.clear()
        result, cache = _retry(process, items, **kwargs)
        assert result == [x * 2 for x in items], expected
        assert calls == expected
        cache.assert_not_called()

    with pytest.raises(RuntimeError, match="returned 1 results for 2"):
        run_with_oom_retry(
            lambda chunk: chunk[:-1], [1, 2], oom_exceptions=(FakeOOM,)
        )


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
    process = _raiser(FakeOOM("CUDA out of memory"))
    with mock.patch("inferio.impl.utils.clear_cache") as cache:
        with pytest.raises(InferenceOOMError) as excinfo:
            run_with_oom_retry(process, ["only"], oom_exceptions=(FakeOOM,))
    assert str(excinfo.value).startswith(OOM_BATCH1_PREFIX)
    assert isinstance(excinfo.value.__cause__, FakeOOM)
    assert cache.call_count == 1, "cleared once per failed attempt"


def test_a_non_oom_exception_propagates_untouched():
    """Conservative on purpose: an exception saying nothing about memory
    must not become a silent halving loop."""
    for failure, match in (
        (ValueError("bad input"), "bad input"),
        (RuntimeError("shape mismatch in forward()"), "shape mismatch"),
    ):
        with pytest.raises(type(failure), match=match):
            run_with_oom_retry(
                _raiser(failure), [1, 2], oom_exceptions=(FakeOOM,)
            )


def test_backends_without_a_cuda_oom_type_still_halve():
    """None of these is a `torch.cuda.OutOfMemoryError`, and on the platforms
    that raise them the halving loop is the only backstop there is."""
    for label, failure in (
        ("memory-error", MemoryError()),
        ("cpu-allocator", RuntimeError(
            "[enforce fail at alloc_cpu.cpp:117] . DefaultCPUAllocator: can't "
            "allocate memory: you tried to allocate 12884901888 bytes.")),
        ("mps", RuntimeError(
            "MPS backend out of memory (MPS allocated: 18.09 GB, max "
            "allowed: 18.13 GB).")),
    ):
        calls = []

        def process(chunk, failure=failure):
            calls.append(len(chunk))
            if len(chunk) > 1:
                raise failure
            return list(chunk)

        result, cache = _retry(process, [1, 2])
        assert result == [1, 2], label
        assert calls == [2, 1, 1], f"{label}: halved to one, then ran the rest"
        assert cache.call_count == 1, label


def test_the_call_records_what_it_actually_executed():
    """The harness asks it whether the impl ran the batch whole, a partly-run
    one being unpriceable; the process total survives a second call."""

    def process(chunk):
        if len(chunk) > 2:
            raise FakeOOM("too big")
        return list(chunk)

    before, ooms_before = last_oom_retry(), total_oom_halvings()
    result, _ = _retry(process, list(range(8)))
    assert result == list(range(8))
    generation, largest, halvings = last_oom_retry()
    assert (largest, halvings) == (2, 2), "8 -> 4 -> 2 before anything ran"
    assert total_oom_halvings() == ooms_before + 2
    if before is not None:
        assert generation > before[0], "each call gets a fresh generation"

    _retry(lambda chunk: list(chunk), list(range(5)))
    assert last_oom_retry()[1:] == (5, 0), "a whole-batch run, no halvings"
    assert total_oom_halvings() == ooms_before + 2

    # The florence2/dots_ocr shape: sub-batched with no OOM, still unpriceable.
    _retry(lambda chunk: list(chunk), list(range(6)), initial_chunk_size=2)
    assert last_oom_retry()[1:] == (2, 0)

    _retry(lambda chunk: list(chunk), [])
    assert last_oom_retry()[1] == 0, "an empty call executed nothing"


def test_fixture_prefix_matches_constant():
    """The torch-free worker-protocol fixture hardcodes the prefix."""
    fixture = (
        Path(__file__).resolve().parents[2]
        / "inferio_worker/fixture_impls/oom_impl.py"
    )
    assert f'"{OOM_BATCH1_PREFIX}' in fixture.read_text(encoding="utf-8")


def test_an_index_ceiling_halves_without_counting_as_an_oom():
    """Size-dependent, so halving is right; not a memory condition, so it must
    not move the halving counter the worker reads as `oom`. At one item it
    propagates rather than becoming an `InferenceOOMError`."""
    calls = []

    def process(chunk):
        calls.append(len(chunk))
        if len(chunk) > 2:
            raise RuntimeError("integer out of range")
        return [x * 10 for x in chunk]

    ooms_before = total_oom_halvings()
    events_before = total_index_limit_events()
    result, cache = _retry(process, list(range(8)))

    assert result == [x * 10 for x in range(8)]
    assert calls == [8, 4, 2, 2, 2, 2]
    assert total_oom_halvings() == ooms_before, "not an out-of-memory condition"
    assert total_index_limit_events() == events_before + 2, "one per halving"
    assert cache.call_count == 0, "nothing here is short of memory"
    assert last_oom_retry()[1:] == (2, 0), (
        "the executed shape is still recorded — that is what makes the batch "
        "unpriceable — but no halving is claimed"
    )

    events_before = total_index_limit_events()
    alone = _raiser(RuntimeError("integer out of range"))
    with pytest.raises(RuntimeError, match="integer out of range"):
        run_with_oom_retry(alone, ["only"], oom_exceptions=(FakeOOM,))
    assert total_index_limit_events() == events_before, "nothing was shrunk"


def test_an_out_of_memory_condition_wins_over_the_ceiling_test():
    """Every OOM test runs first, so an allocator failure mentioning an index
    is still a negative sample."""

    def process(chunk):
        if len(chunk) > 1:
            raise FakeOOM("CUDA out of memory; integer out of range")
        return list(chunk)

    ooms_before = total_oom_halvings()
    events_before = total_index_limit_events()
    _retry(process, [1, 2])
    assert total_oom_halvings() == ooms_before + 1
    assert total_index_limit_events() == events_before


def test_the_ceiling_classifier_is_narrow_where_the_oom_one_is_broad():
    """It also decides a failure is *not* a memory event, so a false positive
    would hide a genuine OOM. It matches only what torch emits."""
    for text in ("integer out of range", "canUse32BitIndexMath(self)"):
        assert looks_like_index_limit(RuntimeError(text)), text
    for text in ("index out of range", "list index out of range", "out of memory"):
        assert not looks_like_index_limit(IndexError(text)), text

    chained = RuntimeError("wrapper")
    chained.__cause__ = RuntimeError("integer out of range")
    assert looks_like_index_limit(chained), "the chain is scanned, as for OOM"
