import logging
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from threading import Lock
from typing import Dict, List, Optional, Set
from inferio.process_model import ProcessIsolatedInferenceModel

InferenceModel = ProcessIsolatedInferenceModel
logger = logging.getLogger(__name__)

def never() -> datetime:
    return datetime.max

class ModelManager:
    _instance: Optional["ModelManager"] = None
    _lock: Lock = Lock()

    def __del__(self) -> None:
        logger.debug("ModelManager deleted")

    def __init__(self) -> None:
        if hasattr(self, "_initialized") and self._initialized:
            return  # Skip reinitialization if already initialized
        logger.debug("Initializing ModelManager")
        self._models: Dict[str, InferenceModel] = {}
        self._lru_caches: Dict[str, OrderedDict[str, datetime]] = defaultdict(
            OrderedDict
        )
        self._cache_key_map: Dict[str, Set[str]] = defaultdict(set)
        self._cache_lock: Lock = Lock()
        self._initialized = True  # Mark the instance as initialized

    def __new__(cls) -> "ModelManager":
        if cls._instance is None:
            logger.debug("Creating ModelManager")
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ModelManager, cls).__new__(cls)
        return cls._instance

    def _remove_from_lru(self, cache_key: str, inference_id: str) -> None:
        """Remove a model from the LRU cache."""
        if inference_id in self._lru_caches[cache_key]:
            logger.debug(
                f"Removing model {inference_id} from cache {cache_key}"
            )
            del self._lru_caches[cache_key][inference_id]
            self._cache_key_map[inference_id].discard(cache_key)
            if not self._cache_key_map[
                inference_id
            ]:  # Unload if no more cache keys reference this model
                self._unload_model(inference_id)

    def _unload_model(self, inference_id: str) -> None:
        """Unload the model when no cache keys reference it."""
        if inference_id in self._models:
            model: InferenceModel = self._models.pop(inference_id)
            logger.debug(f"Unloading model {inference_id}")
            model.unload()
            del self._cache_key_map[inference_id]

    def load_model(
        self,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
    ) -> InferenceModel:
        with self._lock:

            # Update the model in the LRU cache
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

            # Resize LRU cache if necessary before loading the model
            self._resize_lru(cache_key, lru_size)

            # Load the model only after managing the LRU cache
            if inference_id not in self._models:
                try:
                    model_instance = ProcessIsolatedInferenceModel(inference_id)
                    model_instance.load()
                except Exception as e:
                    logger.error(f"Failed to load model {inference_id}: {e}")
                    self._remove_from_lru(cache_key, inference_id)
                    raise e
                self._models[inference_id] = model_instance

            return self._models[inference_id]

    def _resize_lru(self, cache_key: str, lru_size: int) -> None:
        """Ensure the LRU cache does not exceed its size."""
        lru_cache: OrderedDict[str, datetime] = self._lru_caches[cache_key]
        while len(lru_cache) > lru_size:
            oldest_inference_id, _ = lru_cache.popitem(last=False)
            self._cache_key_map[oldest_inference_id].discard(cache_key)
            if not self._cache_key_map[
                oldest_inference_id
            ]:  # Unload if no more cache keys reference this model
                logger.debug(
                    f"{oldest_inference_id} evicted from LRU cache {cache_key}"
                )
                self._unload_model(oldest_inference_id)

    def unload_model(self, cache_key: str, inference_id: str) -> None:
        """Explicitly unload a model and remove it from the cache."""
        with self._lock:
            logger.debug(f"{inference_id} unload requested")
            self._remove_from_lru(cache_key, inference_id)

    def clear_cache(self, cache_key: str) -> None:
        """Clear the entire LRU cache for a specific cache key."""
        with self._lock:
            logger.debug(f"Clearing cache {cache_key}")
            lru_cache: OrderedDict[str, datetime] = self._lru_caches.pop(
                cache_key, OrderedDict()
            )
            for inference_id in lru_cache:
                self._cache_key_map[inference_id].discard(cache_key)
                if not self._cache_key_map[
                    inference_id
                ]:  # Unload if no more cache keys reference this model
                    self._unload_model(inference_id)

    def list_loaded_models(self) -> Dict[str, List[str]]:
        return {
            inference_id: list(cache_keys)
            for inference_id, cache_keys in self._cache_key_map.items()
        }

    def get_ttl_expiration(self, cache_key: str) -> Dict[str, datetime]:
        return dict(self._lru_caches[cache_key])

    def check_ttl_expired(self) -> None:
        """Check for expired TTLs and remove them from the cache."""
        with self._cache_lock:
            for cache_key, lru_cache in self._lru_caches.items():
                expired_models: List[str] = []
                for inference_id, expiration_time in list(lru_cache.items()):
                    if datetime.now() > expiration_time:
                        expired_models.append(inference_id)
                for inference_id in expired_models:
                    logger.debug(f"{inference_id} TTL expired")
                    self._remove_from_lru(cache_key, inference_id)
