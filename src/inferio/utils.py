import base64
import json
import os
from io import BytesIO
from typing import List, Optional, Union

from fastapi import HTTPException, Response, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from inferio.types import PredictionInput


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


def encode_output_response(outputs: List[bytes | dict | list | str]):
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


def parse_input_request(data: str, files: List[UploadFile]):
    parsed_json = json.loads(data)
    inputs: List[Union[dict, str, None]] = parsed_json.get("inputs", [])
    prediction_inputs = [
        PredictionInput(data=item, file=None) for item in inputs
    ]
    if not prediction_inputs:
        raise HTTPException(status_code=400, detail="No inputs provided")

    # Populate PredictionInput objects
    for file in files:
        # Extract the index from the Content-Disposition header
        content_disposition = file.headers.get("content-disposition")
        if not content_disposition:
            raise HTTPException(
                status_code=400,
                detail="Missing Content-Disposition header",
            )
        index = extract_index_from_content_disposition(content_disposition)

        if index is not None and 0 <= index < len(prediction_inputs):
            prediction_inputs[index].file = file.file.read()
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid index {index} in Content-Disposition header",
            )
    return prediction_inputs

def clean_dict(obj: dict) -> dict:
    """
    Recursively converts dictionary values to standard Python types.
    Specifically converts any non-string/dict/bytes iterables to Python lists.
    
    Args:
        obj: The object to clean (dict, list, or other value)
        
    Returns:
        A new object with all custom iterables converted to standard Python types
    """
    # Call the recursive helper function
    ress = clean_dict_inner(obj)
    assert isinstance(ress, dict), "Expected a dictionary as the result"
    return ress 

def clean_dict_inner(obj):
    
    if isinstance(obj, dict):
        return {k: clean_dict_inner(v) for k, v in obj.items()}
    
    # Convert any iterable (but not strings, dicts, or bytes) to a list
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, dict, bytes)):
        return [clean_dict_inner(item) for item in obj]
    
    # Handle nested lists - list comprehension
    elif isinstance(obj, list):
        return [clean_dict_inner(item) for item in obj]
    
    # Base case: return the object itself
    else:
        return obj

def add_cudnn_to_path():
    # Get the absolute path to the inferio directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    # Go up one directory to get to the src directory
    project_root = os.path.dirname(project_root)
    # Go up another directory to get to the project root
    project_root = os.path.dirname(project_root)
    # Define the path to the cudnn directory within the project
    cudnn_path = os.path.join(project_root, "cudnn")
    # Add cudnn/bin directory to the PATH environment variable
    cudnn_bin_path = os.path.join(cudnn_path, "bin")
    os.environ["PATH"] = cudnn_bin_path + os.pathsep + os.environ["PATH"]

    # If you have other directories like include or lib that need to be added, you can add them similarly.
    # For example, if you want to set up the CUDA_PATH to point to your cudnn directory (if needed):
    os.environ["CUDA_PATH"] = cudnn_path

