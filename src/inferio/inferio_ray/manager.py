import asyncio
import logging
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Set

from ray import serve
from ray.serve.handle import DeploymentHandle
from dotenv import load_dotenv

from inferio.config import load_config
from inferio.inferio_ray.create_deployment import build_inference_deployment
from inferio.inferio_ray.deployment_config import get_deployment_config


def never() -> datetime:
    return datetime.max

@serve.deployment(
    name="ModelManager",
    num_replicas=1,
)
class ModelManager:
    def __init__(self) -> None:
        load_dotenv()
        self.logger = logging.getLogger("inferio.ModelManager")
        self.logger.debug("Initializing ModelManager")
        self._handles: Dict[str, DeploymentHandle] = {}
        self._lru_caches: Dict[str, OrderedDict[str, datetime]] = defaultdict(
            OrderedDict
        )
        self._cache_key_map: Dict[str, Set[str]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._config, self._mtime = load_config()
        asyncio.create_task(self._ttl_check_loop())
        asyncio.create_task(self._keepalive_loop())

    async def _ttl_check_loop(self):
        while True:
            await asyncio.sleep(10)
            await self.check_ttl_expired()

    async def get_config(self):
        """Reload the configuration if it has changed."""
        self._config, self._mtime = load_config(self._config, self._mtime)
        return self._config

    async def _keepalive_loop(self):
        while True:
            await asyncio.sleep(5)
            async with self._lock:
                handles = dict(self._handles)
                for inference_id, handle in handles.items():
                    self.logger.debug(f"Sending keepalive to {inference_id}")
                    try:
                        await handle.options(method_name="keepalive").remote()
                    except Exception as e:
                        self.logger.warning(f"Keepalive for {inference_id} failed: {e}")

    async def _remove_from_lru(self, cache_key: str, inference_id: str) -> None:
        """Remove a model from the LRU cache."""
        if inference_id in self._lru_caches[cache_key]:
            self.logger.debug(f"Removing model {inference_id} from cache {cache_key}")
            del self._lru_caches[cache_key][inference_id]
            self._cache_key_map[inference_id].discard(cache_key)
            if not self._cache_key_map.get(inference_id):
                await self._unload_model(inference_id)

    async def _unload_model(self, inference_id: str) -> None:
        """Unload the model when no cache keys reference it."""
        if inference_id in self._handles:
            self._handles.pop(inference_id)
            self.logger.debug(f"Unloading model {inference_id} by deleting application")
            clean_id = inference_id.replace("/", "_")
            serve.delete(f"{clean_id}_app", _blocking=False)
            if inference_id in self._cache_key_map:
                del self._cache_key_map[inference_id]

    async def load_model(
        self,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
    ) -> DeploymentHandle:
        async with self._lock:
            is_new = inference_id not in self._handles
            if is_new:
                self.logger.info(f"Model {inference_id} not loaded. Creating deployment.")
                config = await self.get_config()
                deployment_config = get_deployment_config(inference_id, config)
                handle = build_inference_deployment(inference_id, deployment_config)
                self._handles[inference_id] = handle
            else:
                self.logger.debug(f"Model {inference_id} already loaded. Returning handle.")
                handle = self._handles[inference_id]

            self._cache_key_map[inference_id].add(cache_key)
            if inference_id in self._lru_caches[cache_key]:
                self._lru_caches[cache_key].move_to_end(inference_id)

            # Calculate the new expiration time
            expiration_time = (
                (datetime.now() + timedelta(seconds=ttl_seconds))
                if ttl_seconds >= 0
                else never()
            )
            self._lru_caches[cache_key][inference_id] = expiration_time

            await self._resize_lru(cache_key, lru_size)

            if is_new:
                self.logger.info(f"Calling load() for the first time for {inference_id}")
                await handle.options(method_name="load").remote()

            return handle

    async def _resize_lru(self, cache_key: str, lru_size: int) -> None:
        """Ensure the LRU cache does not exceed its size."""
        lru_cache: OrderedDict[str, datetime] = self._lru_caches[cache_key]
        while len(lru_cache) > lru_size:
            oldest_inference_id, _ = lru_cache.popitem(last=False)
            self._cache_key_map[oldest_inference_id].discard(cache_key)
            if not self._cache_key_map.get(oldest_inference_id):
                self.logger.debug(
                    f"{oldest_inference_id} evicted from LRU cache {cache_key}"
                )
                await self._unload_model(oldest_inference_id)

    async def unload_model(self, cache_key: str, inference_id: str) -> None:
        """Explicitly unload a model and remove it from the cache."""
        async with self._lock:
            self.logger.debug(f"{inference_id} unload requested")
            await self._remove_from_lru(cache_key, inference_id)

    async def clear_cache(self, cache_key: str) -> None:
        """Clear the entire LRU cache for a specific cache key."""
        async with self._lock:
            self.logger.debug(f"Clearing cache {cache_key}")
            if cache_key in self._lru_caches:
                lru_cache = self._lru_caches.pop(cache_key)
                for inference_id in list(lru_cache.keys()):
                    self._cache_key_map[inference_id].discard(cache_key)
                    if not self._cache_key_map.get(inference_id):
                        await self._unload_model(inference_id)

    def list_loaded_models(self) -> Dict[str, List[str]]:
        return {
            inference_id: list(cache_keys)
            for inference_id, cache_keys in self._cache_key_map.items()
        }

    def get_ttl_expiration(self, cache_key: str) -> Dict[str, datetime]:
        return dict(self._lru_caches[cache_key])

    async def check_ttl_expired(self) -> None:
        """Check for expired TTLs and remove them from the cache."""
        async with self._lock:
            now = datetime.now()
            for cache_key, lru_cache in list(self._lru_caches.items()):
                expired_models = [
                    inf_id
                    for inf_id, exp_time in lru_cache.items()
                    if now > exp_time
                ]
                for inference_id in expired_models:
                    self.logger.debug(f"{inference_id} TTL expired in cache {cache_key}")
                    await self._remove_from_lru(cache_key, inference_id)
