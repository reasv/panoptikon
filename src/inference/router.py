import base64
import json
import logging
from io import BytesIO
from typing import Any, Dict, List, Optional, Union

from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Query,
    Response,
    UploadFile,
)
from fastapi.responses import JSONResponse, StreamingResponse

from src.inference.impl.ocr import DoctrModel
from src.inference.impl.wd_tagger import WDTagger
from src.inference.manager import InferenceModel, ModelManager
from src.inference.registry import ModelRegistry, get_base_config_folder
from src.inference.types import PredictionInput

logger = logging.getLogger(__name__)

registry = ModelRegistry(
    base_folder=str(get_base_config_folder()), user_folder="inference_config"
)
registry.register_model("wd_tagger", WDTagger)
registry.register_model("doctr", DoctrModel)

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
    data: str = Form(...),  # The JSON data as a string
    files: List[UploadFile] = File([]),  # The binary files
):
    parsed_json = json.loads(data)
    inputs: List[Union[dict, list, str]] = parsed_json.get("inputs", [])
    prediction_inputs = [
        PredictionInput(data=item, file=None) for item in inputs
    ]
    if not prediction_inputs:
        raise HTTPException(status_code=400, detail="No inputs provided")

    # Populate PredictionInput objects
    for file in files:
        # Extract the index from the Content-Disposition header
        content_disposition = file.headers.get("content-disposition")
        index = extract_index_from_content_disposition(content_disposition)

        if index is not None and 0 <= index < len(prediction_inputs):
            prediction_inputs[index].file = file.file.read()
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid index {index} in Content-Disposition header",
            )

    # Instantiate the model (without loading)
    model_instance: InferenceModel = registry.get_model_instance(
        group, inference_id
    )

    # Load the model with cache key, LRU size, and long TTL to avoid unloading during prediction
    model: InferenceModel = ModelManager().load_model(
        f"{group}/{inference_id}", model_instance, cache_key, lru_size, -1
    )

    logger.debug(
        f"Processing {len(prediction_inputs)} ({len(files)} files) inputs for model {group}/{inference_id}"
    )

    try:
        # Perform prediction
        outputs: List[bytes | dict | list | str] = list(
            model.predict(prediction_inputs)
        )
    except Exception as e:
        logger.error(f"Prediction failed for model {inference_id}: {e}")
        raise HTTPException(status_code=500, detail="Prediction failed")
    finally:
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


def extract_index_from_content_disposition(header: str) -> Optional[int]:
    """Extract the 'index' from the Content-Disposition header."""
    if not header:
        return None
    parts = header.split(";")
    for part in parts:
        part = part.strip()
        if part.startswith("filename="):
            try:
                return int(part.split("=")[1].strip().strip('"'))
            except (IndexError, ValueError):
                return None
    return None


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
