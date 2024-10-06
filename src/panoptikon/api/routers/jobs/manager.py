import logging
import multiprocessing
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from dotenv import load_dotenv
from pydantic import BaseModel

from panoptikon.api.routers.jobs.impl import (
    delete_job_data,
    delete_model_data,
    rescan_folders,
    run_data_extraction_job,
    run_folder_update,
)
from panoptikon.log import setup_logging

logger = logging.getLogger(__name__)

# Define Job Types
JobType = Literal[
    "data_extraction",
    "data_deletion",
    "folder_rescan",
    "folder_update",
    "job_data_deletion",
]


class Job(BaseModel):
    queue_id: int
    job_type: JobType
    conn_args: Dict[str, Any]
    metadata: Optional[str] = None
    log_id: Optional[int] = None
    batch_size: Optional[int] = None
    threshold: Optional[float] = None
    tag: Optional[str] = None


@dataclass
class RunningJob:
    job: Job
    process: multiprocessing.Process


class QueueStatusModel(BaseModel):
    queue: List["JobModel"]


class JobModel(BaseModel):
    queue_id: int
    job_type: JobType
    index_db: str
    metadata: Optional[str] = None
    batch_size: Optional[int] = None
    threshold: Optional[float] = None
    log_id: Optional[int] = None
    running: bool = False
    tag: Optional[str] = None


def execute_job(job: Job):
    load_dotenv()
    setup_logging()
    try:
        if job.job_type == "data_extraction":
            assert job.metadata is not None, "Inference ID is required."
            run_data_extraction_job(
                inference_id=job.metadata,
                batch_size=job.batch_size,
                threshold=job.threshold,
                conn_args=job.conn_args,
            )
        elif job.job_type == "data_deletion":
            assert job.metadata is not None, "Inference ID is required."
            delete_model_data(
                inference_id=job.metadata, conn_args=job.conn_args
            )
        elif job.job_type == "folder_rescan":
            rescan_folders(conn_args=job.conn_args)
        elif job.job_type == "folder_update":
            run_folder_update(
                conn_args=job.conn_args,
            )
        elif job.job_type == "job_data_deletion":
            assert job.log_id is not None, "Log ID is required."
            delete_job_data(log_id=job.log_id, conn_args=job.conn_args)
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
                    index_db=job.conn_args["index_db"],
                    batch_size=job.batch_size,
                    threshold=job.threshold,
                    log_id=job.log_id,
                    running=False,
                    tag=job.tag,
                )
                for job in self.job_queue
            ]
            running = (
                JobModel(
                    queue_id=self.running_job.job.queue_id,
                    job_type=self.running_job.job.job_type,
                    metadata=self.running_job.job.metadata,
                    index_db=self.running_job.job.conn_args["index_db"],
                    batch_size=self.running_job.job.batch_size,
                    threshold=self.running_job.job.threshold,
                    log_id=self.running_job.job.log_id,
                    running=True,
                    tag=self.running_job.job.tag,
                )
                if self.running_job
                else None
            )
            if running:
                queue_list.insert(0, running)
        return QueueStatusModel(queue=queue_list)

    def cancel_queued_jobs(self, queue_ids: List[int]) -> List[int]:
        cancelled = []
        with self.lock:
            for qid in queue_ids:
                # Check if it's the running job
                if self.running_job and self.running_job.job.queue_id == qid:
                    self.cancel_running_job()
                    cancelled.append(qid)
                    continue
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


# To support forward references in Pydantic models
QueueStatusModel.model_rebuild()
JobModel.model_rebuild()
