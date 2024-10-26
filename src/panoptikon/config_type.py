from typing import List, Optional

from pydantic import BaseModel, Field

from panoptikon.db.pql.filters import Match
from panoptikon.db.pql.pql_model import JobFilter
from panoptikon.types import CronJob, JobSettings


class SystemConfig(BaseModel):
    remove_unavailable_files: bool = Field(default=True)
    scan_images: bool = Field(default=True)
    scan_video: bool = Field(default=True)
    scan_audio: bool = Field(default=False)
    scan_html: bool = Field(default=False)
    scan_pdf: bool = Field(default=False)
    enable_cron_job: bool = Field(default=False)
    cron_schedule: str = Field(default="0 3 * * *")
    cron_jobs: List[CronJob] = Field(default_factory=list)
    job_settings: List[JobSettings] = Field(default_factory=list)
    included_folders: List[str] = Field(default_factory=list)
    excluded_folders: List[str] = Field(default_factory=list)
    preload_embedding_models: bool = Field(default=False)
    job_filters: List[JobFilter] = Field(default_factory=list)
    filescan_filter: Optional[Match] = None
