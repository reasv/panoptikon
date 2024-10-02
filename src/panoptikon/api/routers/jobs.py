import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Literal

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
)
from pydantic import BaseModel

from panoptikon.api.routers.jobs_impl import (
    delete_model_data,
    rescan_folders,
    run_data_extraction_job,
    run_folder_update,
)
from panoptikon.api.routers.utils import get_db_readonly, get_db_system_wl
from panoptikon.data_extractors.models import ModelOptsFactory
from panoptikon.db import get_database_connection
from panoptikon.db.config import persist_system_config, retrieve_system_config
from panoptikon.db.extraction_log import get_all_data_logs
from panoptikon.db.files import get_all_file_scans
from panoptikon.db.folders import get_folders_from_database
from panoptikon.types import FileScanRecord, LogRecord, SystemConfig

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/jobs",
    tags=["jobs"],
    responses={404: {"description": "Not found"}},
)


class RunningJob(BaseModel):
    index_db: str
    job_type: Literal[
        "data_extraction", "data_deletion", "folder_rescan", "folder_update"
    ]
    metadata: str | None = None


current_job: RunningJob | None = None


class QueueStatus(BaseModel):
    running_job: RunningJob | None
    queue: None = None


@router.get(
    "/queue",
    summary="Get running job and queue status",
)
def data_job() -> QueueStatus:
    return QueueStatus(running_job=current_job)


def set_running_job(job: RunningJob):
    global current_job
    if current_job:
        raise HTTPException(400, "Job already running")
    current_job = job


@router.post(
    "/data/extraction",
    summary="Run a data extraction job",
)
def data_extraction_job(
    background_tasks: BackgroundTasks,
    inference_id: str = Query(..., title="Inference ID"),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
    set_running_job(
        RunningJob(
            index_db=conn_args["index_db"],
            job_type="data_extraction",
            metadata=inference_id,
        )
    )

    model = ModelOptsFactory.get_model(inference_id)
    background_tasks.add_task(
        run_data_extraction_job,
        model=model,
        conn_args=conn_args,
    )


@router.delete(
    "/data/extraction",
    summary="Delete extracted data",
)
def delete_extracted_data(
    background_tasks: BackgroundTasks,
    inference_id: str = Query(..., title="Inference ID"),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
    set_running_job(
        RunningJob(
            index_db=conn_args["index_db"],
            job_type="data_deletion",
            metadata=inference_id,
        )
    )
    model = ModelOptsFactory.get_model(inference_id)
    background_tasks.add_task(
        delete_model_data,
        model=model,
        conn_args=conn_args,
    )


@router.post(
    "/folders/rescan",
    summary="Run a file scan",
)
def file_scan(
    background_tasks: BackgroundTasks,
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
    set_running_job(
        RunningJob(index_db=conn_args["index_db"], job_type="folder_rescan")
    )
    background_tasks.add_task(
        rescan_folders,
        conn_args=conn_args,
    )


class Folders(BaseModel):
    included_folders: List[str]
    excluded_folders: List[str]


@router.put(
    "/folders",
    summary="Update the folder lists",
)
def update_folders(
    background_tasks: BackgroundTasks,
    folders: Folders = Body(
        ...,
        title="The new sets of included and excluded folders. Replaces the current lists with these.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
    set_running_job(
        RunningJob(index_db=conn_args["index_db"], job_type="folder_update")
    )
    background_tasks.add_task(
        run_folder_update,
        included_folders=folders.included_folders,
        excluded_folders=folders.excluded_folders,
        conn_args=conn_args,
    )


@router.get(
    "/folders",
    summary="Get the current folder lists",
)
def get_folders(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> Folders:
    conn = get_database_connection(**conn_args)
    try:
        current_included_folders = get_folders_from_database(
            conn, included=True
        )
        current_excluded_folders = get_folders_from_database(
            conn, included=False
        )
        return Folders(
            included_folders=current_included_folders,
            excluded_folders=current_excluded_folders,
        )
    finally:
        conn.close()


@router.get(
    "/folders/history",
    summary="Get the scan history",
)
def get_scan_history(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> List[FileScanRecord]:
    conn = get_database_connection(**conn_args)
    try:
        return get_all_file_scans(conn)
    finally:
        conn.close()


@router.get(
    "/data/history",
    summary="Get the extraction history",
)
def get_extraction_history(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> List[LogRecord]:
    conn = get_database_connection(**conn_args)
    try:
        return get_all_data_logs(conn)
    finally:
        conn.close()


@router.put(
    "/config",
    summary="Update the system configuration",
)
def update_config(
    config: SystemConfig = Body(..., title="The new system configuration"),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
    conn = get_database_connection(**conn_args)
    try:
        persist_system_config(conn, config)
        conn.commit()
    finally:
        conn.close()


@router.get(
    "/config",
    summary="Get the current system configuration",
)
def get_config(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> SystemConfig:
    conn = get_database_connection(**conn_args)
    try:
        return retrieve_system_config(conn)
    finally:
        conn.close()
