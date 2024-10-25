import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel

from panoptikon.api.routers.jobs.manager import (
    Job,
    JobManager,
    JobModel,
    QueueStatusModel,
)
from panoptikon.api.routers.utils import get_db_readonly, get_db_system_wl
from panoptikon.config import persist_system_config, retrieve_system_config
from panoptikon.config_type import SystemConfig
from panoptikon.data_extractors.models import ModelOptsFactory
from panoptikon.db import get_database_connection
from panoptikon.db.extraction_log import (
    get_all_data_logs,
    get_setters_total_data,
)
from panoptikon.db.files import get_all_file_scans
from panoptikon.db.folders import get_folders_from_database
from panoptikon.folders import is_resync_needed
from panoptikon.types import FileScanRecord, LogRecord

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/jobs",
    tags=["jobs"],
    responses={404: {"description": "Not found"}},
)

# Initialize FastAPI app and JobManager
job_manager = JobManager()


# Endpoint to get queue status
@router.get(
    "/queue",
    summary="Get running job and queue status",
)
def get_queue_status() -> QueueStatusModel:
    return job_manager.get_queue_status()


# Endpoint to run a data extraction job
@router.post(
    "/data/extraction",
    summary="Run a data extraction job",
)
def enqueue_data_extraction_job(
    inference_ids: List[str] = Query(..., title="Inference ID List"),
    batch_size: Optional[int] = Query(default=None, title="Batch Size"),
    threshold: Optional[float] = Query(
        default=None, title="Confidence Threshold"
    ),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> List[JobModel]:
    jobs = []
    for inference_id in inference_ids:
        def_batch_size, def_threshold = get_default_config(
            inference_id, conn_args["index_db"]
        )
        chosen_batch_size = batch_size
        chosen_threshold = threshold

        if chosen_batch_size is None or chosen_batch_size < 1:
            chosen_batch_size = def_batch_size
        if chosen_threshold is None:
            chosen_threshold = def_threshold
        queue_id = job_manager.get_next_job_id()
        job = Job(
            queue_id=queue_id,
            job_type="data_extraction",
            conn_args=conn_args,
            metadata=inference_id,
            batch_size=chosen_batch_size,
            threshold=chosen_threshold,
        )
        job_manager.enqueue_job(job)
        jobs.append(
            JobModel(
                queue_id=job.queue_id,
                job_type=job.job_type,
                metadata=job.metadata,
                index_db=job.conn_args["index_db"],
                batch_size=job.batch_size,
                threshold=job.threshold,
            )
        )
    return jobs


def get_default_config(
    inference_id: str, index_db: str
) -> Tuple[int, float | None]:
    model = ModelOptsFactory.get_model(inference_id)
    batch_size, threshold = (
        model.default_batch_size(),
        model.default_threshold(),
    )
    system_config = retrieve_system_config(index_db)
    job_settings = system_config.job_settings
    for setting in job_settings:
        if (
            setting.group_name == model.group_name()
            and setting.inference_id == None
        ):
            batch_size = (
                setting.default_batch_size
                if setting.default_batch_size is not None
                else batch_size
            )
            # Model accepts threshold as a parameter
            if model.default_threshold() is not None:
                threshold = (
                    setting.default_threshold
                    if setting.default_threshold is not None
                    else threshold
                )
    for setting in job_settings:
        if (
            setting.group_name == model.group_name()
            and setting.inference_id == model.setter_name()
        ):
            batch_size = (
                setting.default_batch_size
                if setting.default_batch_size is not None
                else batch_size
            )
            # Model accepts threshold as a parameter
            if model.default_threshold() is not None:
                threshold = (
                    setting.default_threshold
                    if setting.default_threshold is not None
                    else threshold
                )
    return batch_size, threshold


# Endpoint to delete extracted data
@router.delete(
    "/data/extraction",
    summary="Delete extracted data",
    status_code=status.HTTP_202_ACCEPTED,
)
def enqueue_delete_extracted_data(
    inference_ids: List[str] = Query(..., title="Inference ID List"),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> List[JobModel]:
    jobs = []
    for inference_id in inference_ids:
        queue_id = job_manager.get_next_job_id()
        job = Job(
            queue_id=queue_id,
            job_type="data_deletion",
            conn_args=conn_args,
            metadata=inference_id,
        )
        job_manager.enqueue_job(job)
        jobs.append(
            JobModel(
                queue_id=job.queue_id,
                job_type=job.job_type,
                metadata=job.metadata,
                index_db=job.conn_args["index_db"],
            )
        )
    return jobs


# Endpoint to run a folder rescan
@router.post(
    "/folders/rescan",
    summary="Run a folder rescan",
    status_code=status.HTTP_202_ACCEPTED,
)
def enqueue_folder_rescan(
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> JobModel:
    queue_id = job_manager.get_next_job_id()
    job = Job(
        queue_id=queue_id,
        job_type="folder_rescan",
        conn_args=conn_args,
    )
    job_manager.enqueue_job(job)
    return JobModel(
        queue_id=job.queue_id,
        job_type=job.job_type,
        metadata=job.metadata,
        index_db=job.conn_args["index_db"],
    )


class Folders(BaseModel):
    included_folders: List[str]
    excluded_folders: List[str]


# Endpoint to update folders
@router.put(
    "/folders",
    summary="Update the database with the current folder lists in the config",
    description="""
Must be run every time after the folder lists in the config are updated,
to ensure that the database is in sync with the config.
If you update the config through the API, this will be done automatically if needed.

This will remove files and items from the database that are no longer in the included folders,
and add files and items that are now in the included folders, as well as remove files and items
from the database that are now in the excluded folders.
""",
    status_code=status.HTTP_202_ACCEPTED,
)
def enqueue_update_folders(
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> JobModel:
    queue_id = job_manager.get_next_job_id()
    job = Job(
        queue_id=queue_id,
        job_type="folder_update",
        conn_args=conn_args,
    )
    job_manager.enqueue_job(job)
    return JobModel(
        queue_id=job.queue_id,
        job_type=job.job_type,
        metadata=None,
        index_db=job.conn_args["index_db"],
    )


class QueueCancelResponse(BaseModel):
    cancelled_jobs: List[int]


# Endpoint to cancel queued jobs
@router.delete(
    "/queue",
    summary="Cancel queued jobs",
    status_code=status.HTTP_200_OK,
)
def cancel_queued_jobs(
    queue_ids: List[int] = Query(..., title="List of Queue IDs to cancel"),
) -> QueueCancelResponse:
    cancelled = job_manager.cancel_queued_jobs(queue_ids)
    if not cancelled:
        raise HTTPException(
            status_code=404, detail="No matching queued jobs found."
        )
    return QueueCancelResponse(cancelled_jobs=cancelled)


class CancelResponse(BaseModel):
    detail: str


# Endpoint to cancel the currently running job
@router.post(
    "/cancel",
    summary="Cancel the currently running job",
    status_code=status.HTTP_200_OK,
)
def cancel_current_job() -> CancelResponse:
    cancelled_job_id = job_manager.cancel_running_job()
    if cancelled_job_id is None:
        raise HTTPException(
            status_code=404, detail="No job is currently running."
        )
    return CancelResponse(detail=f"Job {cancelled_job_id} cancelled.")


# Additional endpoints remain unchanged
@router.get(
    "/folders",
    summary="Get the current folder lists",
    description="""
Get the current included and excluded folders in the database.
These are the folders that are being scanned and not being scanned, respectively.

This list may differ from the config, if the database has not been updated.
""",
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
    page: int | None = Query(1, title="Page number", ge=1),
    page_size: int | None = Query(None, title="Page size", ge=1),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> List[FileScanRecord]:
    conn = get_database_connection(**conn_args)
    try:
        return get_all_file_scans(
            conn,
            page=page,
            page_size=page_size,
        )
    finally:
        conn.close()


@router.delete(
    "/data/history",
    summary="Deletes data generated by the scans given log ids",
)
def delete_scan_data(
    log_ids: List[int] = Query(
        ..., title="List of Log Ids to delete the generated data for"
    ),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> List[JobModel]:
    jobs = []
    for log_id in log_ids:
        job = Job(
            queue_id=job_manager.get_next_job_id(),
            job_type="job_data_deletion",
            conn_args=conn_args,
            log_id=log_id,
        )
        job_manager.enqueue_job(job)
        jobs.append(
            JobModel(
                queue_id=job.queue_id,
                job_type=job.job_type,
                metadata=job.metadata,
                index_db=job.conn_args["index_db"],
                log_id=job.log_id,
            )
        )
    return jobs


@router.get(
    "/data/history",
    summary="Get the extraction history",
)
def get_extraction_history(
    page: int | None = Query(1, title="Page number", ge=1),
    page_size: int | None = Query(None, title="Page size", ge=1),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> List[LogRecord]:
    conn = get_database_connection(**conn_args)
    try:
        return get_all_data_logs(
            conn,
            page=page,
            page_size=page_size,
        )
    finally:
        conn.close()


@router.put(
    "/config",
    summary="Update the system configuration",
    status_code=status.HTTP_200_OK,
)
def update_config(
    config: SystemConfig = Body(..., title="The new system configuration"),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> SystemConfig:
    persist_system_config(conn_args["index_db"], config)
    config = retrieve_system_config(conn_args["index_db"])
    conn = get_database_connection(
        write_lock=False,
        index_db=conn_args["index_db"],
        user_data_db=conn_args["user_data_db"],
    )
    try:
        resync_needed = is_resync_needed(conn, config)
    finally:
        conn.close()
    if resync_needed:
        logger.info(
            "Folder lists changed. Resync needed, scheduling folder update..."
        )
        job_manager.enqueue_job(
            Job(
                queue_id=job_manager.get_next_job_id(),
                job_type="folder_update",
                conn_args=conn_args,
            )
        )
    return config


@router.get(
    "/config",
    summary="Get the current system configuration",
    response_model=SystemConfig,
)
def get_config(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> SystemConfig:
    return retrieve_system_config(conn_args["index_db"])


class SetterDataStats(BaseModel):
    total_counts: List[Tuple[str, int]]


@router.get(
    "/data/setters/total",
    summary="Get the total count of index data entry for each setter",
)
def get_setter_data_count(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> SetterDataStats:
    conn = get_database_connection(**conn_args)
    try:
        return SetterDataStats(total_counts=get_setters_total_data(conn))
    finally:
        conn.close()
