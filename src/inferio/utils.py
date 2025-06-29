import base64
import importlib
import importlib.util
import json
import logging
import os
from io import BytesIO
from pathlib import Path
import pkgutil
from typing import Dict, List, Optional, Union

from fastapi import HTTPException, Response, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput


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

def get_impl_classes(logger: logging.Logger) -> Dict[str, type[InferenceModel]]:
    import inferio.impl
    import sys
    built_ins: Dict[str, type[InferenceModel]] = {}
    # Discover built-in impls
    for finder, name, ispkg in pkgutil.iter_modules(inferio.impl.__path__, inferio.impl.__name__ + "."):
        try:
            mod = importlib.import_module(name)
            if hasattr(mod, "IMPL_CLASS"):
                if isinstance(getattr(mod, "IMPL_CLASS"), type) and issubclass(getattr(mod, "IMPL_CLASS"), InferenceModel):
                    logger.info(f"Found implementation class: {name}.IMPL_CLASS")
                else:
                    logger.warning(f"Module {name} does not have a valid IMPL_CLASS.")
                # Check if the class has a name method
                if hasattr(getattr(mod, "IMPL_CLASS"), "name"):
                    impl_name = getattr(getattr(mod, "IMPL_CLASS"), "name")()
                    if not impl_name:
                        logger.warning(f"Implementation class {name}.IMPL_CLASS has no name method or it returned an empty string. Skipping.")
                        continue
                else:
                    logger.warning(f"Implementation class {name}.IMPL_CLASS does not have a name method. Skipping.")
                    continue  # Skip if no name method is found

                logger.info(f"Implementation class {name}.IMPL_CLASS has name: {impl_name}")
                # Check if the name is already registered
                if impl_name in built_ins:
                    logger.error(f"Built-in implementation class {impl_name} is already registered. Skipping duplicate.")
                    continue
                # Store the class in the built_ins dictionary
                built_ins[impl_name] = getattr(mod, "IMPL_CLASS")
        except Exception as e:
            logger.error(f"Failed to import module inferio.impl.{name}: {e}", exc_info=True)
            pass
    # Discover custom impls
    custom_impl_path = Path(os.environ.get("INFERIO_CUSTOM_IMPL_PATH", "./inferio_custom/"))
    # Ensure __init__.py exists in the custom impl directory
    if custom_impl_path.exists() and custom_impl_path.is_dir():
        init_file = custom_impl_path / "__init__.py"
        if not init_file.exists():
            init_file.touch()
    # Ensure project root is in sys.path for absolute imports
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    custom_impls: Dict[str, type[InferenceModel]] = {}

    if custom_impl_path.exists() and custom_impl_path.is_dir():
        for pyfile in custom_impl_path.glob("*.py"):
            if pyfile.name == "__init__.py":
                continue
            module_name = f"inferio_custom.{pyfile.stem}"
            spec = importlib.util.spec_from_file_location(module_name, pyfile)
            if spec and spec.loader:
                try:
                    mod = importlib.util.module_from_spec(spec)
                    sys.modules[module_name] = mod
                    spec.loader.exec_module(mod)
                    if hasattr(mod, "IMPL_CLASS"):
                        if isinstance(getattr(mod, "IMPL_CLASS"), type) and issubclass(getattr(mod, "IMPL_CLASS"), InferenceModel):
                            logger.info(f"Found custom implementation class: {module_name}.IMPL_CLASS")
                        else:
                            logger.warning(f"Custom module {module_name} does not have a valid IMPL_CLASS.")
                        # Check if the class has a name method
                        if hasattr(getattr(mod, "IMPL_CLASS"), "name"):
                            impl_name = getattr(getattr(mod, "IMPL_CLASS"), "name")()
                            if not impl_name:
                                logger.error(f"Implementation class {module_name}.IMPL_CLASS has no name method or it returned an empty string. Skipping.")
                                continue
                        else:
                            logger.error(f"Implementation class {module_name}.IMPL_CLASS does not have a name method. Skipping.")
                            continue  # Skip if no name method is found

                        logger.info(f"Implementation class {module_name}.IMPL_CLASS has name: {impl_name}")
                        # Check if the name is already registered
                        if impl_name in custom_impls:
                            logger.error(f"Custom implementation class {impl_name} is already registered. Skipping duplicate.")
                            continue
                        # Store the class in the custom_impls dictionary
                        custom_impls[impl_name] = getattr(mod, "IMPL_CLASS")
                except Exception as e:
                    logger.error(f"Failed to import custom module {module_name}: {e}", exc_info=True)
                    pass
    allow_built_in_override = os.environ.get("INFERIO_ALLOW_BUILT_IN_OVERRIDE", "false").lower() in ["true", "1", "yes"]
    if allow_built_in_override:
        logger.info("""
        The INFERIO_ALLOW_BUILT_IN_OVERRIDE environment variable is set to 'true'.
        Built-in implementations can be overridden by custom implementations.
        Custom implementations will take precedence over built-in implementations with the same name().
        """)
    else:
        # Check if there are common keys between the two 
        for key in built_ins.keys():
            if key in custom_impls:
                logger.warning("""
                    The INFERIO_ALLOW_BUILT_IN_OVERRIDE environment variable is not set to 'true'.
                    Built-in implementations cannot be overridden by custom implementations.
                """)
                logger.error(f"Custom implementation {key} overrides built-in implementation. Skipping custom implementation.")
                del custom_impls[key]

    combined_impls: Dict[str, type[InferenceModel]] = {**built_ins, **custom_impls}
    return combined_impls