import itertools
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Sequence, Tuple, Union

from inferio.client.api_client import InferenceAPIClient

logger = logging.getLogger(__name__)


class DistributedInferenceAPIClient:
    """Client that transparently talks to one or many inference servers.

    Args:
        base_urls: Either a single URL or an iterable of URLs.
        weights:   Optional positive numbers, same length as `base_urls`.
                   Bigger number → proportionally more of the batch.
        max_workers: Upper bound on threads used for concurrency.
        retries:  Per‑server HTTP retry count
    """

    def __init__(
        self,
        base_url: Union[str, Sequence[str]],
        weights: Sequence[float] | None = None,
        max_workers: int | None = None,
        retries: int = 3,
    ):
        self._urls: List[str] = (
            [base_url] if isinstance(base_url, str) else list(base_url)
        )
        if not self._urls:
            raise ValueError("At least one base URL is required")

        self._weights = self._normalise_weights(weights)
        self._clients = [
            InferenceAPIClient(url, retries=retries) for url in self._urls
        ]
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers or len(self._urls)
        )

    def predict(
        self,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
        inputs: Sequence[Tuple[str | dict | None, str | bytes | None]],
    ):
        if len(self._clients) == 1:
            # Fast path – keep behaviour identical to old client
            return self._clients[0].predict(
                inference_id, cache_key, lru_size, ttl_seconds, inputs
            )

        # Split the batch
        shards, scatter_map = self._shard_batch(inputs)

        # Run them all concurrently
        futures = {
            self._pool.submit(
                cli.predict,
                inference_id,
                cache_key,
                lru_size,
                ttl_seconds,
                shard,
            ): (cli_ix, shard_ix)
            for cli_ix, (cli, shard_ix, shard) in enumerate(
                zip(self._clients, range(len(shards)), shards)
            )
            if shard  # skip empty shard
        }

        partial_outputs: dict[int, list] = {}
        failed_items: list[Tuple[int, int]] = []  # [(cli_ix, shard_ix)]

        for fut in as_completed(futures):
            cli_ix, shard_ix = futures[fut]
            try:
                partial_outputs[shard_ix] = fut.result()
            except Exception as e:
                logger.warning(
                    "Predict sub‑request failed on %s: %s",
                    self._urls[cli_ix],
                    e,
                )
                failed_items.append((cli_ix, shard_ix))

        # Optional "retry on the healthy servers" pass
        if failed_items and len(partial_outputs) > 0:
            healthy_clients = [
                cli for i, cli in enumerate(self._clients) if i not in
                {cli_ix for cli_ix, _ in failed_items}
            ]
            if healthy_clients:
                logger.info(
                    "Retrying %d shards on %d healthy servers",
                    len(failed_items),
                    len(healthy_clients),
                )
                retry_futs = {
                    self._pool.submit(
                        healthy_clients[retry_idx % len(healthy_clients)].predict,
                        inference_id,
                        cache_key,
                        lru_size,
                        ttl_seconds,
                        shards[shard_ix],
                    ): shard_ix
                    for retry_idx, (_, shard_ix) in enumerate(failed_items)
                }

                for fut in as_completed(retry_futs):
                    shard_ix = retry_futs[fut]
                    partial_outputs[shard_ix] = fut.result()
                    failed_items = [
                        t for t in failed_items if t[1] != shard_ix
                    ]

        if failed_items:
            raise RuntimeError(
                f"{len(failed_items)} sub‑requests failed; aborting batch"
            )

        # Reassemble
        flat_outputs: list = list(
            itertools.chain.from_iterable(
                partial_outputs[i] for i in range(len(shards))
            )
        )
        ordered_outputs = [None] * len(inputs)
        for src_pos, out in zip(scatter_map, flat_outputs):
            ordered_outputs[src_pos] = out
        return ordered_outputs

    def load_model(
        self,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
    ):
        return self._all_or_ignore(
            "load_model",
            inference_id,
            cache_key,
            lru_size,
            ttl_seconds,
        )

    def unload_model(self, inference_id: str, cache_key: str):
        return self._all_or_ignore("unload_model", inference_id, cache_key)

    def clear_cache(self, cache_key: str):
        return self._all_or_ignore("clear_cache", cache_key)

    # Read‑only methods use fail‑over pattern
    def get_cached_models(self):
        return self._first_alive("get_cached_models")

    def get_metadata(self):
        return self._first_alive("get_metadata")

    # Intenal helpers
    # Weight helpers
    def _normalise_weights(self, w: Sequence[float] | None) -> list[float]:
        if w is None:
            return [1.0] * len(self._urls)
        if len(w) != len(self._urls):
            raise ValueError("Weights must match number of URLs")
        if any(x <= 0 for x in w):
            raise ValueError("Weights must be positive")
        total = float(sum(w))
        return [float(x) / total for x in w]

    # Batch splitting
    def _shard_batch(
        self,
        inputs: Sequence[Tuple[Any, Any]],
    ) -> tuple[list[list], list[int]]:
        n = len(inputs)
        shard_sizes = [
            int(n * w) for w in self._weights
        ]  # flooring first pass
        # distribute any remainder
        for i in range(n - sum(shard_sizes)):
            shard_sizes[i % len(shard_sizes)] += 1

        shards: list[list] = []
        scatter_map: list[int] = []  # maps flattened‑shards index → original

        cursor = 0
        for sz in shard_sizes:
            shard = []
            for _ in range(sz):
                shard.append(inputs[cursor])
                scatter_map.append(cursor)
                cursor += 1
            shards.append(shard)
        return shards, scatter_map

    # "fire everywhere, ignore partial failure" pattern
    def _all_or_ignore(self, method_name: str, *args, **kwargs):
        futs = {
            self._pool.submit(
                getattr(cli, method_name), *args, **kwargs
            ): url
            for cli, url in zip(self._clients, self._urls)
        }
        first_ok: Any | None = None
        for fut in as_completed(futs):
            url = futs[fut]
            try:
                result = fut.result()
                first_ok = first_ok or result
            except Exception as e:
                logger.warning("%s failed on %s: %s", method_name, url, e)
        if first_ok is None:
            raise RuntimeError(f"All servers failed {method_name}")
        return first_ok

    # "first server wins, else fail‑over" pattern
    def _first_alive(self, method_name: str, *args, **kwargs):
        for cli, url in zip(self._clients, self._urls):
            try:
                return getattr(cli, method_name)(*args, **kwargs)
            except Exception as e:
                logger.info("%s failed on %s, trying next… (%s)", method_name, url, e)
        raise RuntimeError(f"All servers unavailable for {method_name}")

# -----------------------------------------------------------------------------------------
# USAGE
# -----------------------------------------------------------------------------------------
#
# >>> client = DistributedInferenceAPIClient(
# ...     ["http://gpu‑0.local:9000", "http://gpu‑1.local:9000"],
# ...     weights=[2, 1],   # optional
# ... )
#
# >>> outs = client.predict(inference_id, cache_key, lru_size, ttl_seconds, inputs)

