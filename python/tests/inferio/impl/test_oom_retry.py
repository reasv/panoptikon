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


@pytest.mark.parametrize(
    "failure",
    [
        # The CPU backend's two forms: the builtin, and torch's own allocator
        # message — which never says "out of memory" and is why the
        # classifier carries the explicit spelling.
        MemoryError(),
        RuntimeError(
            "[enforce fail at alloc_cpu.cpp:117] . DefaultCPUAllocator: can't "
            "allocate memory: you tried to allocate 12884901888 bytes."
        ),
        # MPS's, which the generic substring already covers.
        RuntimeError(
            "MPS backend out of memory (MPS allocated: 18.09 GB, other "
            "allocations: 384.00 KB, max allowed: 18.13 GB)."
        ),
    ],
    ids=["memory-error", "cpu-allocator", "mps"],
)
def test_backends_without_a_cuda_oom_type_still_halve(failure):
    """The negative-signal widening (docs/unified-memory-admission.md).

    None of these is a `torch.cuda.OutOfMemoryError`, and on the platforms
    that raise them the halving loop is the *only* backstop there is — the
    orchestrator's admission is what keeps batches inside the budget, and
    this is what catches the case where it was wrong.
    """
    calls = []

    def process(chunk):
        calls.append(len(chunk))
        if len(chunk) > 1:
            raise failure
        return list(chunk)

    result, cache = _retry(process, [1, 2])
    assert result == [1, 2]
    assert calls == [2, 1, 1], "halved to one item, then ran the rest"
    assert cache.call_count == 1


def test_a_plain_runtime_error_is_not_treated_as_an_oom():
    """The widening is text-classified, so it has to be conservative: a
    generic `RuntimeError` that says nothing about memory must propagate
    rather than be retried at half the batch size, or every impl bug becomes
    a silent halving loop."""

    def process(chunk):
        raise RuntimeError("shape mismatch in forward()")

    with pytest.raises(RuntimeError, match="shape mismatch"):
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


# ---------------------------------------------------------------------------
# The shape ceiling: halves like an OOM, is never reported as one (run2 S1)
# ---------------------------------------------------------------------------


def test_an_index_ceiling_halves_without_counting_as_an_oom():
    """`RuntimeError("integer out of range")` is `at::native::safe_downcast`
    refusing to launch a kernel over more elements than a signed 32-bit int
    can address — the failure run2 measured easyOCR's CRAFT detector hitting
    at batch 29 with 3 GiB of a 96 GiB board still free.

    It is size-dependent, so halving is right. It is not a memory condition,
    so it must not move the halving counter the worker reads as `oom`: a
    negative sample here would deflate a model that has plenty of memory.
    """
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
    assert cache.call_count == 0, (
        "nothing here is short of memory; emptying the allocator would cost a "
        "synchronise for no reason"
    )
    generation, largest, halvings = last_oom_retry()
    assert (largest, halvings) == (2, 0), (
        "the executed shape is still recorded — that is what makes the batch "
        "unpriceable — but no halving is claimed"
    )


def test_an_index_ceiling_on_a_single_item_propagates_untouched():
    """No smaller batch to try, and it is not an out-of-memory condition, so
    it must not be re-raised as `InferenceOOMError`: the caller's own
    fallback has to see it for what it is."""
    def process(chunk):
        raise RuntimeError("integer out of range")

    events_before = total_index_limit_events()
    with pytest.raises(RuntimeError, match="integer out of range"):
        run_with_oom_retry(process, ["only"], oom_exceptions=(FakeOOM,))
    assert total_index_limit_events() == events_before, (
        "nothing was retried, so nothing was shrunk"
    )


def test_an_out_of_memory_condition_wins_over_the_ceiling_test():
    """Order matters and is deliberate: every out-of-memory test runs first,
    so a genuine allocator failure whose text happens to mention an index is
    still the negative sample the deflation path exists for."""
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
    """Both decide "retry at half the size", but this one also decides a
    failure is *not* a memory event, and a false positive there would hide a
    genuine out-of-memory condition. So it matches only what torch emits."""
    for text in ("integer out of range", "canUse32BitIndexMath(self)"):
        assert looks_like_index_limit(RuntimeError(text)), text
    for text in ("index out of range", "list index out of range", "out of memory"):
        assert not looks_like_index_limit(IndexError(text)), text

    chained = RuntimeError("wrapper")
    chained.__cause__ = RuntimeError("integer out of range")
    assert looks_like_index_limit(chained), "the chain is scanned, as for OOM"
