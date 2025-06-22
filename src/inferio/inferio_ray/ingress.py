import asyncio
import logging
from typing import Any, Dict, List

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from ray import serve
from ray.serve.handle import DeploymentHandle

from inferio.config import list_inference_ids, load_config
from inferio.cudnnsetup import cudnn_setup
from inferio.inferio_ray.create_deployment import build_inference_deployment
from inferio.inferio_ray.deployment_config import get_deployment_config
from inferio.inferio_ray.manager import ModelManager
from inferio.utils import encode_output_response, parse_input_request

load_dotenv()
cudnn_setup()

app = FastAPI(
    tags=["inference"],
    responses={404: {"description": "Not found"}},
)

@dataclass
class StatusResponse:
    status: str

@dataclass
class CacheListResponse:
    cache: Dict[str, List[str]]

class CacheKeyResponse(BaseModel):
    expirations: Dict[str, str]

@serve.deployment(
    name="InferioIngress",
    autoscaling_config={
        "min_replicas": 1,
    }
)
@serve.ingress(app)
class InferioIngress:
    def __init__(self, manager_handle: DeploymentHandle):
        from dotenv import load_dotenv
        from panoptikon.log import setup_logging

        load_dotenv()
        setup_logging()
        self.logger = logging.getLogger("inferio.ingress")
        self.logger.info("Ingress Deployment initialized")
        self._handles: dict[str, DeploymentHandle] = {}
        self._lock = asyncio.Lock()
        self._config, self._mtime = load_config()
        self.manager = manager_handle

    async def get_config(self):
        """Reload the configuration if it has changed."""
        self._config, self._mtime = load_config(self._config, self._mtime)
        return self._config
    
    async def _ensure(self, inference_id: str):
        if inference_id in self._handles:
            return self._handles[inference_id]

        async with self._lock:
            if inference_id in self._handles:
                return self._handles[inference_id]
            
            self.logger.info(f"Building deployment for {inference_id}")
            deployment_config = get_deployment_config(inference_id, await self.get_config())
            handle = build_inference_deployment(inference_id, deployment_config)
            self._handles[inference_id] = handle
            return handle

    @app.post(
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
    async def predict(
        self,
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
        full_inference_id = f"{group}/{inference_id}"
        self.logger.debug(
            f"Processing {len(inputs)} ({len(files)} files) inputs for model {full_inference_id}"
        )
        try:
            # Perform prediction
            model_handle = await self._ensure(full_inference_id)
            await self.manager.options(method_name="load_model").remote(
                full_inference_id, model_handle, cache_key, lru_size, ttl_seconds
            )
            outputs: List[bytes | dict | list | str] = list(await model_handle.remote(inputs))
        except Exception as e:
            self.logger.error(f"Prediction failed for model {inference_id}: {e}")
            raise HTTPException(status_code=500, detail="Prediction failed")
        return encode_output_response(outputs)


    @app.put(
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
    async def load_model(
        self,
        group: str,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
    ):
        try:
            full_inference_id = f"{group}/{inference_id}"
            model_handle = await self._ensure(full_inference_id)
            await self.manager.options(method_name="load_model").remote(
                full_inference_id, model_handle, cache_key, lru_size, ttl_seconds
            )
            return StatusResponse(status="loaded")
        except Exception as e:
            self.logger.error(f"Failed to load model {inference_id}: {e}")
            raise HTTPException(status_code=500, detail="Failed to load model")


    @app.delete(
        "/cache/{cache_key}/{group}/{inference_id}",
        summary="Unload a model from memory",
        description="""
    Removes a model from the LRU cache `cache_key`.
    Once a model is removed from all caches, it will be unloaded from memory.
        """,
        response_model=StatusResponse,
    )
    async def unload_model(self, cache_key: str, group: str, inference_id: str):
        full_inference_id = f"{group}/{inference_id}"
        await self.manager.options(method_name="unload_model").remote(cache_key, full_inference_id)
        return StatusResponse(status="unloaded")


    @app.delete(
        "/cache/{cache_key}",
        summary="Clear the cache",
        description="""
    Clears the LRU cache with key `cache_key`.
    If the models in it are not referenced by any other cache, they will be unloaded from memory.
        """,
        response_model=StatusResponse,
    )
    async def clear_cache(self, cache_key: str):
        await self.manager.options(method_name="clear_cache").remote(cache_key)
        return StatusResponse(status="cleared")

    @app.get(
        "/cache/{cache_key}",
        summary="Get expiration times for all models in a cache",
        description="Returns a mapping of `inference_id`s for all models in the cache to their expiration times.",
    )
    async def get_cache_expiration(self, cache_key: str) -> CacheKeyResponse:
        expire_dict = await self.manager.options(method_name="get_ttl_expiration").remote(cache_key)
        return CacheKeyResponse(
            expirations={
                inference_id: expiration_time.isoformat()
                for inference_id, expiration_time in expire_dict.items()
            }
        )

    @app.get(
        "/cache",
        summary="Get the list of loaded models",
        description="Returns a mapping of `inference_id`s for all loaded models to the lists of `cache_key`s that reference them.",
        response_model=CacheListResponse,
    )
    async def list_caches(self) -> CacheListResponse:
        cache = await self.manager.options(method_name="list_loaded_models").remote()
        return CacheListResponse(cache=cache)

    @app.get(
        "/metadata",
        summary="Get a mapping of all available models and their metadata",
        description="Returns metadata for all available `inference_id`s, divided by group.",
        response_model=Dict[str, Dict[str, Any]],
    )
    async def get_metadata(self) -> Dict[str, Dict[str, Any]]:
        config = await self.get_config()
        return list_inference_ids(config)


manager_app = ModelManager.bind()
serve_app = InferioIngress.bind(manager_app) # type: ignore