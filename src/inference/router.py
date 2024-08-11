import base64
import logging
from io import BytesIO
from typing import Any, Dict, List, Union

from fastapi import (
    APIRouter,
    Body,
    File,
    HTTPException,
    Query,
    Response,
    UploadFile,
)
from fastapi.responses import JSONResponse, StreamingResponse

from src.inference.manager import InferenceModel, ModelManager
from src.inference.registry import ModelRegistry, get_base_config_folder
from src.inference.types import PredictionInput

logger = logging.getLogger(__name__)

registry = ModelRegistry(
    base_folder=str(get_base_config_folder()), user_folder="inference_config"
)

router = APIRouter(
    prefix="/inference",
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
    inputs: Union[List[Union[dict, str]], None] = Body(None),  # JSON inputs
    file_inputs: List[UploadFile] = File(
        []
    ),  # Optional file inputs (multipart form-data)
):
    # Ensure that if both inputs and file_inputs are provided, they have the same length
    if inputs and file_inputs:
        if len(inputs) != len(file_inputs):
            raise HTTPException(
                status_code=400,
                detail="JSON inputs and file inputs must have the same length.",
            )

    # Instantiate the model (without loading)
    model_instance: InferenceModel = registry.get_model_instance(
        group, inference_id
    )

    # Load the model with cache key, LRU size, and long TTL to avoid unloading during prediction
    model: InferenceModel = ModelManager().load_model(
        f"{group}/{inference_id}", model_instance, cache_key, lru_size, 6000
    )

    # Process inputs into a list of PredictionInput objects
    processed_inputs = []

    if inputs and file_inputs:
        # Combine JSON and file inputs into PredictionInput instances
        for data_input, file_input in zip(inputs, file_inputs):
            file_data = file_input.file.read()
            processed_inputs.append(
                PredictionInput(data=data_input, file=file_data)
            )
    elif inputs:
        for data_input in inputs:
            processed_inputs.append(PredictionInput(data=data_input, file=None))
    elif file_inputs:
        for file_input in file_inputs:
            file_data = file_input.file.read()
            processed_inputs.append(PredictionInput(data=None, file=file_data))

    # Perform prediction
    outputs: List[bytes | dict | list | str] = model.predict(processed_inputs)

    # Update the model's TTL after the prediction is made
    ModelManager().load_model(
        f"{group}/{inference_id}",
        model_instance,
        cache_key,
        lru_size,
        ttl_seconds,
    )

    # Handle the outputs by returning a streaming response if there is only one binary output
    if len(outputs) == 1 and isinstance(outputs[0], bytes):
        return StreamingResponse(
            BytesIO(outputs[0]), media_type="application/octet-stream"
        )

    # Check if all outputs are binary
    if all(isinstance(output, bytes) for output in outputs):
        # Return a multipart response with all binary outputs
        boundary = "multipart-boundary"
        multipart_data = []

        for idx, output in enumerate(outputs):
            part_headers = f'--{boundary}\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename="output{idx}.bin"\r\n\r\n'.encode(
                "utf-8"
            )
            assert isinstance(output, bytes), "Output must be bytes"
            multipart_data.append(part_headers + output + b"\r\n")

        multipart_data.append(f"--{boundary}--\r\n".encode("utf-8"))
        return Response(
            content=b"".join(multipart_data),
            media_type=f"multipart/mixed; boundary={boundary}",
        )

    # Handle the outputs by encoding binary data if necessary
    encoded_outputs = []
    for output in outputs:
        if isinstance(output, (str, dict, list)):
            # Directly append JSON-serializable outputs
            encoded_outputs.append(output)
        elif isinstance(output, bytes):
            # Encode binary data to base64 for safe JSON transport
            encoded_outputs.append(
                {
                    "__type__": "base64",
                    "content": base64.b64encode(output).decode("utf-8"),
                }
            )
        else:
            raise HTTPException(
                status_code=500, detail="Unexpected output type from the model."
            )

    return JSONResponse(content={"outputs": encoded_outputs})


@router.put("/load/{group}/{inference_id}")
def load_model(
    group: str,
    inference_id: str,
    cache_key: str,
    lru_size: int,
    ttl_seconds: int,
) -> Dict[str, str]:
    try:
        model_instance: InferenceModel = registry.get_model_instance(
            group, inference_id
        )
        ModelManager().load_model(
            f"{group}/{inference_id}",
            model_instance,
            cache_key,
            lru_size,
            ttl_seconds,
        )
        return {"status": "loaded"}
    except Exception as e:
        logger.error(f"Failed to load model {inference_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to load model")


@router.put("/unload/{group}/{inference_id}")
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
async def list_loaded_models() -> Dict[str, List[str]]:
    return ModelManager().list_loaded_models()


@router.get("/metadata")
async def list_model_metadata() -> Dict[str, Dict[str, Any]]:
    return registry.list_inference_ids()


@router.post("/check_ttl")
async def check_ttl() -> Dict[str, str]:
    ModelManager().check_ttl_expired()
    return {"status": "ttl checked"}
