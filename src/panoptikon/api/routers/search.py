import logging

from fastapi import APIRouter, Depends
from pydantic.dataclasses import dataclass as pydantic_dataclass

from panoptikon.db import get_database_connection
from panoptikon.db.search import search_files
from panoptikon.db.search.types import SearchQuery, SearchQueryModel

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/search",
    responses={404: {"description": "Not found"}},
)


@router.get("/")
def process_endpoint(data: SearchQueryModel = Depends()):
    conn = get_database_connection(write_lock=False)
    results = list(search_files(conn, SearchQuery(**data.__dict__)))
    file_res = [res for res, count in results if res]
    count = results[0][1]
    return {
        "count": count,
        "results": file_res,
    }
