import logging
import multiprocessing
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
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

# Define Job Types
JobType = Literal[
    "data_extraction",
    "data_deletion",
    "folder_rescan",
    "folder_update",
]


class Job(BaseModel):
    queue_id: int
    job_type: JobType
    conn_args: Dict[str, Any]
    metadata: Optional[str] = None
    included_folders: Optional[List[str]] = None
    excluded_folders: Optional[List[str]] = None


@dataclass
class RunningJob:
    job: Job
    process: multiprocessing.Process


class QueueStatusModel(BaseModel):
    running_job: Optional["RunningJobModel"]
    queue: List["JobModel"]


class RunningJobModel(BaseModel):
    queue_id: int
    job_type: JobType
    metadata: Optional[str] = None


class JobModel(BaseModel):
    queue_id: int
    job_type: JobType
    metadata: Optional[str] = None


def execute_job(job: Job):
    try:
        if job.job_type == "data_extraction":
            assert job.metadata is not None, "Inference ID is required."
            model = ModelOptsFactory.get_model(job.metadata)
            run_data_extraction_job(model=model, conn_args=job.conn_args)
        elif job.job_type == "data_deletion":
            assert job.metadata is not None, "Inference ID is required."
            model = ModelOptsFactory.get_model(job.metadata)
            delete_model_data(model=model, conn_args=job.conn_args)
        elif job.job_type == "folder_rescan":
            rescan_folders(conn_args=job.conn_args)
        elif job.job_type == "folder_update":
            assert (
                job.included_folders is not None
                and job.excluded_folders is not None
            ), "Both included and excluded folders are required."
            run_folder_update(
                included_folders=job.included_folders,
                excluded_folders=job.excluded_folders,
                conn_args=job.conn_args,
            )
        else:
            logger.error(f"Unknown job type: {job.job_type}")
    except Exception as e:
        logger.error(f"Job {job.queue_id} failed with error: {e}")


class JobManager:
    def __init__(self):
        self.job_queue: List[Job] = []
        self.running_job: Optional[RunningJob] = None
        self.queued_jobs: Dict[int, Job] = {}
        self.job_counter: int = 0
        self.lock = threading.Lock()
        self.worker_thread = threading.Thread(
            target=self.job_consumer, daemon=True
        )
        self.worker_thread.start()
        logger.info("JobManager initialized and worker thread started.")

    def get_next_job_id(self) -> int:
        with self.lock:
            self.job_counter += 1
            return self.job_counter

    def enqueue_job(self, job: Job):
        with self.lock:
            self.job_queue.append(job)
            self.queued_jobs[job.queue_id] = job
            logger.info(f"Enqueued job {job.queue_id}: {job.job_type}")

    def job_consumer(self):
        while True:
            job = None
            with self.lock:
                if self.running_job is None and self.job_queue:
                    job = self.job_queue.pop(0)
                    self.queued_jobs.pop(job.queue_id, None)
                    logger.info(f"Dequeued job {job.queue_id}: {job.job_type}")

            if job:
                process = multiprocessing.Process(
                    target=execute_job, args=(job,)
                )
                running_job = RunningJob(job=job, process=process)
                with self.lock:
                    self.running_job = running_job
                logger.info(
                    f"Starting job {job.queue_id} in process {process.pid}"
                )
                process.start()
                process.join()
                with self.lock:
                    if (
                        self.running_job
                        and self.running_job.job.queue_id == job.queue_id
                    ):
                        logger.info(f"Job {job.queue_id} completed.")
                        self.running_job = None
            else:
                # No job to process, sleep briefly to prevent tight loop
                threading.Event().wait(1)

    def get_queue_status(self) -> QueueStatusModel:
        with self.lock:
            queue_list = [
                JobModel(
                    queue_id=job.queue_id,
                    job_type=job.job_type,
                    metadata=job.metadata,
                )
                for job in self.job_queue
            ]
            running = (
                RunningJobModel(
                    queue_id=self.running_job.job.queue_id,
                    job_type=self.running_job.job.job_type,
                    metadata=self.running_job.job.metadata,
                )
                if self.running_job
                else None
            )
        return QueueStatusModel(running_job=running, queue=queue_list)

    def cancel_queued_jobs(self, queue_ids: List[int]) -> List[int]:
        cancelled = []
        with self.lock:
            for qid in queue_ids:
                job = self.queued_jobs.pop(qid, None)
                if job and job in self.job_queue:
                    self.job_queue.remove(job)
                    cancelled.append(qid)
                    logger.info(f"Cancelled queued job {qid}: {job.job_type}")
        return cancelled

    def cancel_running_job(self) -> Optional[int]:
        with self.lock:
            if self.running_job:
                pid = self.running_job.process.pid
                self.running_job.process.terminate()
                self.running_job.process.join()
                logger.info(
                    f"Cancelled running job {self.running_job.job.queue_id} with PID {pid}"
                )
                completed_job_id = self.running_job.job.queue_id
                self.running_job = None
                return completed_job_id
            else:
                return None


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
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> List[JobModel]:
    jobs = []
    for inference_id in inference_ids:
        queue_id = job_manager.get_next_job_id()
        job = Job(
            queue_id=queue_id,
            job_type="data_extraction",
            conn_args=conn_args,
            metadata=inference_id,
        )
        job_manager.enqueue_job(job)
        jobs.append(
            JobModel(
                queue_id=job.queue_id,
                job_type=job.job_type,
                metadata=job.metadata,
            )
        )
    return jobs


# Endpoint to delete extracted data
@router.delete(
    "/data/extraction",
    summary="Delete extracted data",
    status_code=status.HTTP_202_ACCEPTED,
)
def enqueue_delete_extracted_data(
    inference_ids: str = Query(..., title="Inference ID List"),
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
        queue_id=job.queue_id, job_type=job.job_type, metadata=job.metadata
    )


class Folders(BaseModel):
    included_folders: List[str]
    excluded_folders: List[str]


# Endpoint to update folders
@router.put(
    "/folders",
    summary="Update the folder lists",
    status_code=status.HTTP_202_ACCEPTED,
)
def enqueue_update_folders(
    folders: Folders = Body(
        ...,
        title="The new sets of included and excluded folders. Replaces the current lists with these.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> JobModel:
    queue_id = job_manager.get_next_job_id()
    job = Job(
        queue_id=queue_id,
        job_type="folder_update",
        conn_args=conn_args,
        included_folders=folders.included_folders,
        excluded_folders=folders.excluded_folders,
    )
    job_manager.enqueue_job(job)
    return JobModel(queue_id=job.queue_id, job_type=job.job_type, metadata=None)


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


class ConfigResponse(BaseModel):
    detail: str


@router.put(
    "/config",
    summary="Update the system configuration",
    status_code=status.HTTP_200_OK,
)
def update_config(
    config: SystemConfig = Body(..., title="The new system configuration"),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
) -> ConfigResponse:
    conn = get_database_connection(**conn_args)
    try:
        persist_system_config(conn, config)
        conn.commit()
    finally:
        conn.close()
    return ConfigResponse(detail="System configuration updated.")


@router.get(
    "/config",
    summary="Get the current system configuration",
    response_model=SystemConfig,
)
def get_config(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> SystemConfig:
    conn = get_database_connection(**conn_args)
    try:
        return retrieve_system_config(conn)
    finally:
        conn.close()


# To support forward references in Pydantic models
QueueStatusModel.model_rebuild()
RunningJobModel.model_rebuild()
JobModel.model_rebuild()
