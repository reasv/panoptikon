import json
import logging
from re import T
from typing import Any, Dict, List, Sequence, Tuple, Union

import requests as r
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger(__name__)

class InferenceAPIClient:
    def __init__(self, base_url: str, retries: int = 3):
        self.base_url = base_url
        self.session = self._create_session(retries)

    def _create_session(self, retries: int) -> r.Session:
        session = r.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=1,
            status_forcelist=[429, 502, 503, 504],
            allowed_methods=[
                "HEAD",
                "GET",
                "POST",
                "PUT",
                "DELETE",
                "OPTIONS",
                "TRACE",
            ],
            raise_on_status=True,
            connect=retries,  # retry on connection errors
            read=None,  # no retry on read timeouts
            redirect=3,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def predict(
        self,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
        inputs: Sequence[Tuple[str | dict | None, str | bytes | None]],
    ):
        url = f"{self.base_url}/predict/{inference_id}"
        params = {
            "cache_key": cache_key,
            "lru_size": lru_size,
            "ttl_seconds": ttl_seconds,
        }
        json_data = {"inputs": [item[0] for item in inputs]}
        data = {"data": json.dumps(json_data)}
        files = process_input_files([item[1] for item in inputs])

        response = self.session.post(url, params=params, data=data, files=files)
        if response.status_code == 200:
            result = handle_predict_resp(response)
            return result
        else:
            logger.error(
                f"Prediction Fail (Status: {response.status_code})",
            )
            logger.debug(f"Response content: {response.content}")
            response.raise_for_status()
            raise ValueError("Unexpected response")

    def load_model(
        self,
        inference_id: str,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/load/{inference_id}"
        params = {
            "cache_key": cache_key,
            "lru_size": lru_size,
            "ttl_seconds": ttl_seconds,
        }
        return handle_resp(self.session.put(url, params=params))

    def unload_model(
        self,
        inference_id: str,
        cache_key: str,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/cache/{cache_key}/{inference_id}"
        return handle_resp(self.session.delete(url))

    def clear_cache(
        self,
        cache_key: str,
    ) -> Dict[str, Any]:
        return handle_resp(
            self.session.delete(f"{self.base_url}/cache/{cache_key}")
        )

    def get_cached_models(self) -> Dict[str, Any]:
        return handle_resp(self.session.get(f"{self.base_url}/cache"))

    def get_metadata(self) -> Dict[str, dict]:
        return handle_resp(self.session.get(f"{self.base_url}/metadata"))


def handle_resp(response: Response):
    if response.status_code == 200:
        return response.json()
    else:
        logger.debug(f"Response content: {response.content}")
        response.raise_for_status()
        raise ValueError("Unexpected response")


def handle_predict_resp(
    response: Response,
) -> Union[List[Dict[str, Any]], List[bytes]]:
    try:
        content_type = response.headers.get("Content-Type", "")

        # Check if the response is JSON
        if "application/json" in content_type:
            return response.json()[
                "outputs"
            ]  # Parse JSON response as a dictionary

        # Check if the response is multipart
        elif "multipart/mixed" in content_type:
            return parse_multipart_response(response)

        # Check if the response is a single binary output
        elif "application/octet-stream" in content_type and response.content:
            return [response.content]
        else:
            raise ValueError(f"Unexpected content type: {content_type}")
    except ValueError as e:
        logger.error(f"Error decoding predict response: ValueError: {e}")
        raise e
    except r.RequestException as e:
        logger.error(f"Error decoding predict response: RequestException: {e}")
        raise e


def parse_multipart_response(response: Response) -> List[bytes]:
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

    files_list = [files[i] for i in range(len(files))]
    return files_list


def process_input_files(files: List[str | bytes | None]):
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
