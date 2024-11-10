import logging
from typing import BinaryIO, Optional

import aiohttp
import requests

from .containers import SauceResponse
from .errors import (
    BadFileSizeError,
    BadKeyError,
    LongLimitReachedError,
    ShortLimitReachedError,
    UnknownApiError,
    UnknownClientError,
    UnknownServerError,
)
from .params import DB, BgColor, Hide, _OutputType

logger = logging.getLogger(__name__)


class SauceNao:
    SAUCENAO_URL = "https://saucenao.com/search.php"

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        testmode: int = 0,
        dbmask: Optional[int] = None,
        dbmaski: Optional[int] = None,
        db: int = DB.ALL,
        numres: int = 6,
        frame: int = 1,
        hide: int = Hide.NONE,
        bgcolor: str = BgColor.NONE,
    ) -> None:

        params = dict()

        if api_key is not None:
            params["api_key"] = api_key
        if dbmask is not None:
            params["dbmask"] = dbmask
        if dbmaski is not None:
            params["dbmaski"] = dbmaski

        params["testmode"] = testmode
        params["db"] = db
        params["numres"] = numres
        params["hide"] = hide
        params["frame"] = frame
        params["bgcolor"] = bgcolor  # from https://saucenao.com/testing/
        params["output_type"] = _OutputType.JSON
        self.params = params

    def from_file(self, file: BinaryIO) -> SauceResponse:
        return self._search(self.params, {"file": file})

    def from_url(self, url: str) -> SauceResponse:
        params = self.params.copy()
        params["url"] = url
        return self._search(params)

    def _search(self, params, files=None):
        resp = requests.post(self.SAUCENAO_URL, params=params, files=files)
        status_code = resp.status_code

        if status_code == 200:
            raw = self._verify_response(resp, params)
            return SauceResponse(raw)

        # Taken from https://saucenao.com/tools/examples/api/identify_images_v1.1.py
        # Actually server returns 200 and user_id=0 if key is bad
        elif status_code == 403:
            raise BadKeyError("Invalid API key")

        elif status_code == 413:
            raise BadFileSizeError("File is too large")

        elif status_code == 429:
            if "Daily" in resp.json()["header"]["message"]:
                raise LongLimitReachedError("24 hours limit reached")
            logger.debug(f"429 Response from SauceNao: {resp.text}")
            raise ShortLimitReachedError("30 seconds limit reached")

        raise UnknownApiError(f"Server returned status code {status_code}")

    @staticmethod
    def _verify_response(resp, params):
        parsed_resp = resp.json()
        resp_header = parsed_resp["header"]

        status = resp_header["status"]
        user_id = int(resp_header["user_id"])

        # Taken from https://saucenao.com/tools/examples/api/identify_images_v1.1.py
        if status < 0:
            raise UnknownClientError("Unknown client error, status < 0")
        elif status > 0:
            raise UnknownServerError("Unknown API error, status > 0")
        elif user_id < 0:
            raise UnknownServerError("Unknown API error, user_id < 0")

        # Request passed, but api_key was ignored
        elif user_id == 0 and "api_key" in params:
            raise BadKeyError("Invalid API key")

        long_remaining = resp_header["long_remaining"]
        short_remaining = resp_header["short_remaining"]

        # Taken from https://saucenao.com/tools/examples/api/identify_images_v1.1.py
        if short_remaining < 0:
            raise ShortLimitReachedError("30 seconds limit reached")
        elif long_remaining < 0:
            raise LongLimitReachedError("24 hours limit reached")

        return parsed_resp


class AIOSauceNao(SauceNao):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._session = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def from_file(self, file: BinaryIO) -> SauceResponse:  # type: ignore
        return await self._search(self.params, {"file": file})

    async def from_url(self, url: str) -> SauceResponse:  # type: ignore
        params = self.params.copy()
        params["url"] = url
        return await self._search(params)

    async def _search(self, params, files=None):  # type: ignore
        session = self._session or aiohttp.ClientSession()

        async with session.post(
            self.SAUCENAO_URL, params=params, data=files
        ) as resp:
            status_code = resp.status

            # close only if not called via 'async with AIOSauceNao(...)'
            if not self._session:
                await session.close()

            if status_code == 200:
                parsed_resp = await resp.json()
                raw = self._verify_response(parsed_resp, params)

                return SauceResponse(raw)

            # Taken from https://saucenao.com/tools/examples/api/identify_images_v1.1.py
            # Actually server returns 200 and user_id=0 if key is bad
            elif status_code == 403:
                raise BadKeyError("Invalid API key")

            elif status_code == 413:
                raise BadFileSizeError("File is too large")

            elif status_code == 429:
                parsed_resp = await resp.json()
                if "Daily" in parsed_resp["header"]["message"]:
                    raise LongLimitReachedError("24 hours limit reached")
                raise ShortLimitReachedError("30 seconds limit reached")

            raise UnknownApiError(f"Server returned status code {status_code}")

    @staticmethod
    def _verify_response(resp, params):
        resp_header = resp["header"]

        status = resp_header["status"]
        user_id = int(resp_header["user_id"])

        # Taken from https://saucenao.com/tools/examples/api/identify_images_v1.1.py
        if status < 0:
            raise UnknownClientError("Unknown client error, status < 0")
        elif status > 0:
            raise UnknownServerError("Unknown API error, status > 0")
        elif user_id < 0:
            raise UnknownServerError("Unknown API error, user_id < 0")

        # Request passed, but api_key was ignored
        elif user_id == 0 and "api_key" in params:
            raise BadKeyError("Invalid API key")

        long_remaining = resp_header["long_remaining"]
        short_remaining = resp_header["short_remaining"]

        # Taken from https://saucenao.com/tools/examples/api/identify_images_v1.1.py
        if short_remaining < 0:
            raise ShortLimitReachedError("30 seconds limit reached")
        elif long_remaining < 0:
            raise LongLimitReachedError("24 hours limit reached")

        return resp
