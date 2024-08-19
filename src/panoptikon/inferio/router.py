import logging
from typing import Any, Dict, List

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi_utilities.repeat.repeat_every import repeat_every

from panoptikon.inferio.impl.clip import ClipModel
from panoptikon.inferio.impl.ocr import DoctrModel
from panoptikon.inferio.impl.sentence_transformers import (
    SentenceTransformersModel,
)
from panoptikon.inferio.impl.wd_tagger import WDTagger
from panoptikon.inferio.impl.whisper import FasterWhisperModel
from panoptikon.inferio.manager import InferenceModel, ModelManager
from panoptikon.inferio.registry import ModelRegistry
from panoptikon.inferio.utils import encode_output_response, parse_input_request

logger = logging.getLogger(__name__)

ModelRegistry.set_user_folder("config/inference")
ModelRegistry.register_model(WDTagger)
ModelRegistry.register_model(DoctrModel)
ModelRegistry.register_model(SentenceTransformersModel)
ModelRegistry.register_model(FasterWhisperModel)
ModelRegistry.register_model(ClipModel)

router = APIRouter(
    prefix="/api/inference",
    tags=["inference"],
    responses={404: {"description": "Not found"}},
)


@router.post("/predict/{group}/{inference_id}")
def predict(
    group: str,
    inference_id: str,
    cache_key: str = Query(...),
    lru_size: int = Query(...),
    ttl_seconds: int = Query(...),
    data: str = Form(...),  # The JSON data as a string
    files: List[UploadFile] = File([]),  # The binary files
):
    inputs = parse_input_request(data, files)
    logger.debug(
        f"Processing {len(inputs)} ({len(files)} files) inputs for model {group}/{inference_id}"
    )

    # Load the model with cache key, LRU size, and long TTL to avoid unloading during prediction
    model: InferenceModel = ModelManager().load_model(
        f"{group}/{inference_id}", cache_key, lru_size, -1
    )

    try:
        # Perform prediction
        outputs: List[bytes | dict | list | str] = list(model.predict(inputs))
    except Exception as e:
        logger.error(f"Prediction failed for model {inference_id}: {e}")
        raise HTTPException(status_code=500, detail="Prediction failed")
    finally:
        # Update the model's TTL after the prediction is made
        ModelManager().load_model(
            f"{group}/{inference_id}",
            cache_key,
            lru_size,
            ttl_seconds,
        )

    return encode_output_response(outputs)


@router.put("/load/{group}/{inference_id}")
def load_model(
    group: str,
    inference_id: str,
    cache_key: str,
    lru_size: int,
    ttl_seconds: int,
) -> Dict[str, str]:
    try:
        ModelManager().load_model(
            f"{group}/{inference_id}",
            cache_key,
            lru_size,
            ttl_seconds,
        )
        return {"status": "loaded"}
    except Exception as e:
        logger.error(f"Failed to load model {inference_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to load model")


@router.delete("/cache/{cache_key}/{group}/{inference_id}")
def unload_model(
    group: str,
    inference_id: str,
    cache_key: str,
) -> Dict[str, str]:
    ModelManager().unload_model(cache_key, f"{group}/{inference_id}")
    return {"status": "unloaded"}


@router.delete("/cache/{cache_key}")
def clear_cache(cache_key: str) -> Dict[str, str]:
    ModelManager().clear_cache(cache_key)
    return {"status": "cache cleared"}


@router.get("/cache")
async def get_cached_models() -> Dict[str, List[str]]:
    return ModelManager().list_loaded_models()


@router.get("/metadata")
async def get_metadata() -> Dict[str, Dict[str, Any]]:
    return ModelRegistry().list_inference_ids()


@repeat_every(seconds=10, logger=logger)
def check_ttl():
    """Check the TTL of all loaded models and unload expired ones.
    Should be called periodically to ensure that models are not kept in memory indefinitely.
    """
    ModelManager().check_ttl_expired()
