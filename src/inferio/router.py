import logging
from typing import Any, Dict, List

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi_utilities.repeat.repeat_every import repeat_every
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from inferio.impl.clip import ClipModel
from inferio.impl.danbooru import DanbooruTagger
from inferio.impl.florence2 import Florence2
from inferio.impl.ocr import DoctrModel
from inferio.impl.sentence_transformers import SentenceTransformersModel
from inferio.impl.wd_tagger import WDTagger
from inferio.impl.whisper import FasterWhisperModel
from inferio.manager import InferenceModel, ModelManager
from inferio.registry import ModelRegistry
from inferio.utils import (
    add_cudnn_to_path,
    encode_output_response,
    parse_input_request,
)

add_cudnn_to_path()
logger = logging.getLogger(__name__)

ModelRegistry.set_user_folder("config/inference")
ModelRegistry.register_model(WDTagger)
ModelRegistry.register_model(DoctrModel)
ModelRegistry.register_model(SentenceTransformersModel)
ModelRegistry.register_model(FasterWhisperModel)
ModelRegistry.register_model(ClipModel)
ModelRegistry.register_model(Florence2)
ModelRegistry.register_model(DanbooruTagger)

router = APIRouter(
    prefix="/api/inference",
    tags=["inference"],
    responses={404: {"description": "Not found"}},
)


@router.post(
    "/predict/{group}/{inference_id}",
    summary="Run batch inference on a model by its `inference_id`",
    description="""
Runs batch inference on a model by its `inference_id` with the given inputs.

Before inference, the model is loaded using the specified `cache_key`, LRU size, and TTL (in seconds).
This is identical to calling `PUT /load/{group}/{inference_id}`, see the documentation on that endpoint for more details.

Binary inputs are provided as multipart form data, structured inputs as JSON, the JSON string must be in the `data` form field.
The JSON in the data field must be an object with an `inputs` key containing an array with the number of elements matching the size of the batch.
Each element in the array can be a string, a dictionary, or null (in case that batch element only has binary input).

A batch consists of multiple inputs, each of which can be a binary file or a structured input or both.
Binary files are mapped to the structured input by their filename which must be an index corresponding to the index of a structured input in the JSON array.
The exact format depends on the specific model being used.
The output can be either a JSON object containing an array under the key "outputs", a multipart/mixed response for binary data,
or a single application/octet-stream for a single binary output.

Binary outputs are usually embeddings, which are provided in the npy format and can be loaded with numpy.load.

See `inferio.client` for an example of how to use this endpoint, which is non-trivial due to the multipart form data input and output.
""",
)
def predict(
    group: str,
    inference_id: str,
    cache_key: str = Query(...),
    lru_size: int = Query(...),
    ttl_seconds: int = Query(...),
    data: str = Form(
        ...,
        description="""
A JSON string containing the list of inputs to the batch prediction function, with the following structure:
```json
{
    "inputs": [
        {"input2": "value"},
        null,
        ...
    ]
}
```
The array must have the same length as the number of inputs in the batch.
This means that each file you include in the request must have a corresponding entry in the array.
Entries can be JSON objects or null. For example, text embeddings expect objects with the following structure:
```json
{
    "inputs": [
        {"text": "This is a sentence."},
        {"text": "Another sentence."},
        ...
    ]
}
```
For some models, the inputs can be null, in which case the corresponding file will be the only input.

Often, when not required, the input object will be used to pass optional inference-time parameters, such as "confidence" for confidence thresholds.
In that case, null would result in default values being used.

Filenames for files in the files parameter must be integers starting from 0, representing the index in the batch, matching an element in the `inputs` array.
""",
    ),  # The JSON data as a string
    files: List[UploadFile] = File(
        [],
        description="""
A list of binary files to include in the batch prediction.
Each file must have a filename that is an integer starting from 0, representing the index in the batch, matching an element in the `inputs` array in the `data` field.
Files may be optional depending on the model, some do not operate on binary data. 
""",
    ),  # The binary files
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


@dataclass
class StatusResponse:
    status: str


@router.put(
    "/load/{group}/{inference_id}",
    summary="Ensure a model is loaded into memory",
    description="""
Loads a model into memory with the specified `cache_key`, LRU size, and TTL (in seconds).
As long as the model is present in at least one LRU cache, it will be kept in memory.

Models are evicted from an LRU in four cases:

- The LRU's size is exceeded when another load is attempted, causing the least recently used model(s) to be evicted
- The model's TTL expires
- The LRU is explicitly cleared by `cache_key` (see DELETE /cache/{cache_key})
- The model is explicitly removed from the LRU (see DELETE /cache/{cache_key}/{group}/{inference_id})

The model will be loaded into memory only if it is not already loaded.
If the model is already loaded, the cache key, LRU size, and TTL will be updated.
The LRU size is overridden any time a load request is made, which may evict models from the LRU when it is resized.

A TTL of -1 means the model will never be unloaded due to TTL expiration. Other conditions still apply.
    """,
    response_model=StatusResponse,
)
def load_model(
    group: str,
    inference_id: str,
    cache_key: str,
    lru_size: int,
    ttl_seconds: int,
):
    try:
        ModelManager().load_model(
            f"{group}/{inference_id}",
            cache_key,
            lru_size,
            ttl_seconds,
        )
        return StatusResponse(status="loaded")
    except Exception as e:
        logger.error(f"Failed to load model {inference_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to load model")


@router.delete(
    "/cache/{cache_key}/{group}/{inference_id}",
    summary="Unload a model from memory",
    description="""
Removes a model from the LRU cache `cache_key`.
Once a model is removed from all caches, it will be unloaded from memory.
    """,
    response_model=StatusResponse,
)
def unload_model(
    group: str,
    inference_id: str,
    cache_key: str,
):
    ModelManager().unload_model(cache_key, f"{group}/{inference_id}")
    return StatusResponse(status="unloaded")


@router.delete(
    "/cache/{cache_key}",
    summary="Clear the cache",
    description="""
Clears the LRU cache with key `cache_key`.
If the models in it are not referenced by any other cache, they will be unloaded from memory.
    """,
    response_model=StatusResponse,
)
def clear_cache(cache_key: str):
    ModelManager().clear_cache(cache_key)
    return StatusResponse(status="cleared")


class CacheKeyResponse(BaseModel):
    expirations: Dict[str, str]


@router.get(
    "/cache/{cache_key}",
    summary="Get expiration times for all models in a cache",
    description="Returns a mapping of `inference_id`s for all models in the cache to their expiration times.",
)
async def get_cache_expiration(cache_key: str) -> CacheKeyResponse:
    expire_dict = ModelManager().get_ttl_expiration(cache_key)
    return CacheKeyResponse(
        expirations={
            inference_id: expiration_time.isoformat()
            for inference_id, expiration_time in expire_dict.items()
        }
    )


@dataclass
class CacheListResponse:
    cache: Dict[str, List[str]]


@router.get(
    "/cache",
    summary="Get the list of loaded models",
    description="Returns a mapping of `inference_id`s for all loaded models to the lists of `cache_key`s that reference them.",
    response_model=CacheListResponse,
)
async def get_cached_models():
    return CacheListResponse(cache=ModelManager().list_loaded_models())


@router.get(
    "/metadata",
    summary="Get a mapping of all available models and their metadata",
    description="Returns metadata for all available `inference_id`s, divided by group.",
    response_model=Dict[str, Dict[str, Any]],
)
def get_metadata() -> Dict[str, Dict[str, Any]]:
    return ModelRegistry().list_inference_ids()


@repeat_every(seconds=10, logger=logger)
async def check_ttl():
    """Check the TTL of all loaded models and unload expired ones.
    Should be called periodically to ensure that models are not kept in memory indefinitely.
    """
    ModelManager().check_ttl_expired()
