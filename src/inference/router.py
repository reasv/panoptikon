import logging
from typing import Any, Dict, List

from fastapi import APIRouter

from src.inference.manager import BaseModel, ModelManager
from src.inference.registry import ModelRegistry, get_base_config_folder

logger = logging.getLogger(__name__)

registry = ModelRegistry(
    base_folder=str(get_base_config_folder()), user_folder="inference_config"
)


router = APIRouter(
    prefix="/inference",
    tags=["inference"],
    responses={404: {"description": "Not found"}},
)


@router.post("/predict/{inference_id}")
def predict(
    inference_id: str,
    inputs: List[Any],
    cache_key: str,
    lru_size: int,
    ttl_seconds: int,
) -> List[Any]:
    # Instantiate the model (without loading)
    model_instance: BaseModel = registry.get_model_instance(inference_id)

    # Load the model with cache key, LRU size, and TTL
    model: BaseModel = ModelManager().load_model(
        inference_id, model_instance, cache_key, lru_size, ttl_seconds
    )

    # Perform prediction
    outputs: List[Any] = model.predict(inputs)

    # Call load model again, in order to make sure TTL is updated
    # after the prediction is made
    model: BaseModel = ModelManager().load_model(
        inference_id, model_instance, cache_key, lru_size, ttl_seconds
    )

    return outputs


@router.post("/unload/{cache_key}/{inference_id}")
def unload_model(cache_key: str, inference_id: str) -> Dict[str, str]:
    ModelManager().unload_model(cache_key, inference_id)
    return {"status": "unloaded"}


@router.post("/clear_cache/{cache_key}")
def clear_cache(cache_key: str) -> Dict[str, str]:
    ModelManager().clear_cache(cache_key)
    return {"status": "cache cleared"}


@router.get("/list")
async def list_models() -> Dict[str, List[str]]:
    return ModelManager().list_loaded_models()


@router.post("/check_ttl")
async def check_ttl() -> Dict[str, str]:
    ModelManager().check_ttl_expired()
    return {"status": "ttl checked"}
