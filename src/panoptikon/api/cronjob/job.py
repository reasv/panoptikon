import logging
from typing import List

from panoptikon.api.routers.jobs.manager import Job
from panoptikon.api.routers.jobs.router import job_manager
from panoptikon.api.routers.utils import get_db_system_wl
from panoptikon.config import retrieve_system_config
from panoptikon.data_extractors.models import ModelOptsFactory
from panoptikon.types import CronJob

logger = logging.getLogger(__name__)


def run_cronjob(index_db: str):
    try:
        logger.info("Running cronjob")
        conn_args = get_db_system_wl(index_db, None)
        job_tag = f"cronjob[{index_db}]"
        job_manager.enqueue_job(
            Job(
                queue_id=job_manager.get_next_job_id(),
                job_type="folder_rescan",
                conn_args=conn_args,
                tag=job_tag,
            )
        )
        system_config = retrieve_system_config(index_db)
        src_jobs: List[CronJob] = []
        derived_data_jobs: List[CronJob] = []
        for scheduled_job in system_config.cron_jobs:
            model = ModelOptsFactory.get_model(scheduled_job.inference_id)
            if model.target_entities == ["items"]:
                src_jobs.append(scheduled_job)
            else:
                derived_data_jobs.append(scheduled_job)

        ordered_jobs = src_jobs + derived_data_jobs

        for scheduled_job in ordered_jobs:
            logger.info(
                f"Scheduling a job for {scheduled_job.inference_id} (DB: {index_db})"
            )
            job_manager.enqueue_job(
                Job(
                    queue_id=job_manager.get_next_job_id(),
                    job_type="data_extraction",
                    conn_args=conn_args,
                    metadata=scheduled_job.inference_id,
                    batch_size=scheduled_job.batch_size,
                    threshold=scheduled_job.threshold,
                    tag=job_tag,
                )
            )
    except Exception as e:
        logger.error(f"Error running cronjob: {e}")
