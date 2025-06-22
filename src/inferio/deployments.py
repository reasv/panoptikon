import os
import asyncio
from typing import Any, Dict, List
from fastapi import FastAPI
import ray
from ray import serve
from ray.serve.handle import DeploymentHandle
import logging
from inferio.config import get_model_config, list_inference_ids, load_config
from inferio.impl.utils import get_device
from inferio.inferio_types import PredictionInput
from inferio.model import InferenceModel
from inferio.utils import encode_output_response, parse_input_request
os.environ["RAY_TMPDIR"] = "Q:/projects/panoptikon/.ray_tmp"
def build_inference_deployment(inference_id: str, global_config: Dict[str, Any]):
    config = get_model_config(inference_id, global_config)
    impl_class_name = config.pop("impl_class", None)
    if impl_class_name is None:
        raise ValueError(f"Model class name not found in config for inference_id: {inference_id}")
    devices = get_device()
    max_replicas = config.pop(
        "max_replicas",
        len(devices)
    )
    batch_wait_timeout_s = config.pop(
        "batch_wait_timeout_s", 
        float(os.getenv("BATCH_WAIT_TIMEOUT_S", "0.1"))
    )
    max_batch_size = config.pop(
        "max_batch_size",
        int(os.getenv("MAX_BATCH_SIZE", "64"))
    )
    clean_id= inference_id.replace("/", "_")

    @serve.deployment(
        name=f"{clean_id}_deployment",
        ray_actor_options={
            "num_cpus": 0.1,
        },
        autoscaling_config={
            "min_replicas": 0,
            "max_replicas": max_replicas,
            "initial_replicas": 1,
            "target_ongoing_requests": 2,
            "upscale_delay_s": 10,
            "downscale_delay_s": 30
        }
    )
    class InferenceDeployment:
        logger: logging.Logger
        model: InferenceModel
        def __init__(self):
            """Initialize the inference deployment."""
            import logging
            from dotenv import load_dotenv
            from inferio.utils import get_impl_classes
            from panoptikon.log import setup_logging
            load_dotenv()
            setup_logging()
            self.logger = logging.getLogger(f"deployments.{inference_id}")
            impl_classes = get_impl_classes(self.logger)
            for cls in impl_classes:
                if cls.name() == impl_class_name:
                    self.model = cls(**config)
                    break
            else:
                raise ValueError(f"Model class {impl_class_name} not found in impl_classes")
            self.logger.info(f"[{inference_id}] init in PID {os.getpid()} with impl_class {impl_class_name}")
        
        @serve.batch(max_batch_size=max_batch_size, batch_wait_timeout_s=batch_wait_timeout_s)
        async def __call__(self, inputs: List[PredictionInput]) -> List[bytes | dict | list | str]:
            self.logger.debug(f"Received {len(inputs)} batch inputs")
            return list(self.model.predict(inputs))

        @serve.batch(max_batch_size=max_batch_size, batch_wait_timeout_s=batch_wait_timeout_s)
        async def predict(self, inputs: List[PredictionInput]) -> List[bytes | dict | list | str]:
            self.logger.debug(f"Received {len(inputs)} inputs for prediction")
            return list(self.model.predict(inputs))
        
        async def load(self) -> None:
            """Load the model."""
            self.logger.info(f"Loading model")
            self.model.load()
        
        async def keepalive(self) -> None:
            """Keep the model alive."""
            self.logger.info(f"Keeping model alive")
            # This can be used to keep the model loaded or perform any periodic tasks.
        
    app = InferenceDeployment.bind()
    handle = serve.run(app, name=f"{clean_id}_app", blocking=False, route_prefix=None)

    return handle

@serve.deployment(
    name="ModelRouter",
    ray_actor_options={
        "num_cpus": 0.1,
    },
    autoscaling_config={
        "min_replicas": 1,
        "initial_replicas": 1,
        "target_ongoing_requests": 2,
        "upscale_delay_s": 10,
        "downscale_delay_s": 10
    }
)
class ModelRouter:
    def __init__(self):
        import logging
        from dotenv import load_dotenv
        from panoptikon.log import setup_logging
        load_dotenv()
        setup_logging()
        self._handles: dict[str, DeploymentHandle] = {}
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger("ModelRouter")
        self._config, self._mtime = load_config()
        self.logger.info(f"ModelRouter initialized")

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
            handle = build_inference_deployment(inference_id, self._config)
            self._handles[inference_id] = handle
            return handle

    async def __call__(self, inference_id: str, inputs: List[PredictionInput]) -> List[bytes | dict | list | str]:
        h = await self._ensure(inference_id)
        return await h.remote(inputs)
    
    async def load(self, inference_id: str) -> None:
        """Load the model for the given inference ID."""
        h = await self._ensure(inference_id)
        h = h.options(method_name="load")
        await h.remote()

    async def keepalive(self, inference_id: str) -> None:
        """Keep the model alive for the given inference ID."""
        h = await self._ensure(inference_id)
        h = h.options(method_name="keepalive")
        await h.remote()

import logging
import os
from typing import Any, Dict, List

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi_utilities.repeat.repeat_every import repeat_every
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

app = FastAPI(
    prefix="/api/inference",
    tags=["inference"],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger(__name__)
@dataclass
class StatusResponse:
    status: str

@dataclass
class CacheListResponse:
    cache: Dict[str, List[str]]



class CacheKeyResponse(BaseModel):
    expirations: Dict[str, str]
@serve.deployment
@serve.ingress(app)
class FastAPIDeployment:
    def __init__(self, router: DeploymentHandle):
        import logging
        from dotenv import load_dotenv
        from panoptikon.log import setup_logging
        load_dotenv()
        setup_logging()
        self.logger = logging.getLogger("inferio")
        self.logger.info("FastAPIDeployment initialized")
        self.router = router
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
        logger.debug(
            f"Processing {len(inputs)} ({len(files)} files) inputs for model {group}/{inference_id}"
        )

        try:
            # Perform prediction
            outputs: List[bytes | dict | list | str] = list(await self.router.remote(f"{group}/{inference_id}", inputs))
        except Exception as e:
            logger.error(f"Prediction failed for model {inference_id}: {e}")
            raise HTTPException(status_code=500, detail="Prediction failed")
        finally:
            pass
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
            await self.router.options(method_name="load").remote(f"{group}/{inference_id}")
            return StatusResponse(status="loaded")
        except Exception as e:
            logger.error(f"Failed to load model {inference_id}: {e}")
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
    async def unload_model(
        self,
        group: str,
        inference_id: str,
        cache_key: str,
    ):
        # ModelManager().unload_model(cache_key, f"{group}/{inference_id}")
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
        # ModelManager().clear_cache(cache_key)
        return StatusResponse(status="cleared")

    @app.get(
        "/cache/{cache_key}",
        summary="Get expiration times for all models in a cache",
        description="Returns a mapping of `inference_id`s for all models in the cache to their expiration times.",
    )
    async def get_cache_expiration(self, cache_key: str) -> CacheKeyResponse:
        #expire_dict = ModelManager().get_ttl_expiration(cache_key)
        return CacheKeyResponse(
            expirations={
                # inference_id: expiration_time.isoformat()
                # for inference_id, expiration_time in expire_dict.items()
            }
        )

    @app.get(
        "/cache",
        summary="Get the list of loaded models",
        description="Returns a mapping of `inference_id`s for all loaded models to the lists of `cache_key`s that reference them.",
        response_model=CacheListResponse,
    )
    async def get_cached_models(self):
        return CacheListResponse(cache={})

    @app.get(
        "/metadata",
        summary="Get a mapping of all available models and their metadata",
        description="Returns metadata for all available `inference_id`s, divided by group.",
        response_model=Dict[str, Dict[str, Any]],
    )
    def get_metadata(self) -> Dict[str, Dict[str, Any]]:
        config, mtime = load_config()
        return list_inference_ids(config)


ray_app = FastAPIDeployment.bind(ModelRouter.bind()) # type: ignore