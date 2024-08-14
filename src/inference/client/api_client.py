import json
import logging
from typing import Any, Dict, List, Optional, Union

import requests as r
from requests import Response

logger = logging.getLogger(__name__)


class InferenceAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def predict(
        self,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
        inputs: list,
    ):
        url = f"{self.base_url}/predict/{inference_id}"
        # Prepare the query parameters
        params = {
            "cache_key": cache_key,
            "lru_size": lru_size,
            "ttl_seconds": ttl_seconds,
        }
        # For each input, if it's not a tuple, convert it to a tuple
        inputs = [
            (
                item
                if isinstance(item, tuple)
                else (
                    (item, None)
                    if not isinstance(item, bytes)
                    else (None, item)
                )
            )
            for item in inputs
        ]
        # Prepare the JSON data payload
        json_data = {"inputs": [item[0] for item in inputs]}
        # Convert the JSON data to a string and add it to the form data
        data = {"data": json.dumps(json_data)}
        # Prepare the file uploads
        files = process_input_files([item[1] for item in inputs])

        # Make the POST request
        response = r.post(url, params=params, data=data, files=files)
        # Handle the response
        if response.status_code == 200:
            result = handle_predict_resp(response)
            return result
        else:
            logger.error(
                f"Prediction Fail (Status: {response.status_code})",
            )
            logger.debug("Response content:", response.content)
            response.raise_for_status()

    def load_model(
        self,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
    ):
        url = f"{self.base_url}/load/{inference_id}"
        params = {
            "cache_key": cache_key,
            "lru_size": lru_size,
            "ttl_seconds": ttl_seconds,
        }
        handle_resp(r.put(url, params=params))

    def unload_model(
        self,
        inference_id: str,
        cache_key: str,
    ):
        url = f"{self.base_url}/cache/{cache_key}/{inference_id}"
        handle_resp(r.delete(url))

    def clear_cache(
        self,
        cache_key: str,
    ):
        handle_resp(r.delete(f"{self.base_url}/cache/{cache_key}"))

    def get_cached_models(self):
        handle_resp(r.get(f"{self.base_url}/cache"))

    def get_metadata(self):
        handle_resp(r.get(f"{self.base_url}/metadata"))


def handle_resp(response: Response):
    if response.status_code == 200:
        return response.json()
    else:
        logger.debug("Response content:", response.content)
        response.raise_for_status()


def handle_predict_resp(
    response: Response,
) -> Union[Dict[str, Any], Dict[int, bytes]]:
    try:
        content_type = response.headers.get("Content-Type", "")

        # Check if the response is JSON
        if "application/json" in content_type:
            return response.json()  # Parse JSON response as a dictionary

        # Check if the response is multipart
        elif "multipart/mixed" in content_type:
            return parse_multipart_response(response)

        # Check if the response is a single binary output
        elif "application/octet-stream" in content_type and response.content:
            return {0: response.content}
        else:
            raise ValueError(f"Unexpected content type: {content_type}")
    except ValueError as e:
        logger.error(f"Error decoding predict response: ValueError: {e}")
        raise e
    except r.RequestException as e:
        logger.error(f"Error decoding predict response: RequestException: {e}")
        raise e


def parse_multipart_response(response: Response) -> Dict[int, bytes]:
    cont_type = response.headers.get("Content-Type")
    assert cont_type, "Content-Type header not found in response"
    boundary = cont_type.split("boundary=")[1]
    parts = response.content.split(f"--{boundary}".encode("utf-8"))

    files: Dict[int, bytes] = {}
    for part in parts:
        if part and b"Content-Type" in part:
            headers, content = part.split(b"\r\n\r\n", 1)
            content_disposition = [
                header
                for header in headers.split(b"\r\n")
                if b"Content-Disposition" in header
            ][0]
            filename = (
                content_disposition.decode("utf-8")
                .split('filename="')[1]
                .split('"')[0]
            )
            # Extract the index from the filename
            index = int(filename.replace("output", "").replace(".bin", ""))
            files[index] = content.rstrip(b"\r\n")

    return files


def process_input_files(files: List[str | bytes]):
    if not files:
        return None
    input_files = []
    for i, file_item in enumerate(files):
        if file_item is not None:
            # Determine if the file_item is a path or bytes
            if isinstance(file_item, str):
                input_files.append(
                    (
                        "files",
                        (
                            f"{i}",
                            open(file_item, "rb"),
                            "application/octet-stream",
                        ),
                    )
                )
            elif isinstance(file_item, bytes):
                input_files.append(
                    (
                        "files",
                        (
                            f"{i}",
                            file_item,
                            "application/octet-stream",
                        ),
                    )
                )
    return input_files
