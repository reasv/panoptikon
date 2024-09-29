import datetime
import logging
from typing import Any, Dict, List

from fastapi import APIRouter, BackgroundTasks, Body, Depends, Path
from pydantic import BaseModel

from panoptikon.api.routers.utils import get_db_readonly, get_db_system_wl
from panoptikon.data_extractors.extraction_jobs.types import (
    ExtractionJobProgress,
    ExtractionJobReport,
)
from panoptikon.data_extractors.models import ModelOpts
from panoptikon.db import get_database_connection
from panoptikon.db.config import persist_system_config, retrieve_system_config
from panoptikon.db.extraction_log import get_all_data_logs
from panoptikon.db.files import get_all_file_scans
from panoptikon.db.folders import get_folders_from_database
from panoptikon.db.search.types import OrderType
from panoptikon.db.utils import vacuum_database
from panoptikon.folders import rescan_all_folders, update_folder_lists
from panoptikon.types import FileScanRecord, LogRecord, SystemConfig

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/jobs",
    tags=["jobs"],
    responses={404: {"description": "Not found"}},
)

def run_data_extraction_job(
    model: ModelOpts,
    conn_args: Dict[str, Any],
):
    conn = get_database_connection(**conn_args)
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        failed, images, videos, other, units = [], 0, 0, 0, 0
        start_time = datetime.datetime.now()
        for progress in model.run_extractor(conn):
            if type(progress) == ExtractionJobProgress:
                # Job is in progress
                pass
                # progress_tracker(
                #     (progress.processed_items, progress.total_items),
                #     desc=(
                #         f"ETA: {progress.eta_string} | "
                #         + f"Last Item: {shorten_path(progress.item.path)}"
                #     ),
                #     unit="files",
                # )
            elif type(progress) == ExtractionJobReport:
                # Job is complete
                images = progress.images
                videos = progress.videos
                failed = progress.failed_paths
                other = progress.other
                units = progress.units

        end_time = datetime.datetime.now()
        total_time = end_time - start_time
        total_time_pretty = str(total_time).split(".")[0]
        conn.commit()
        failed_str = "\n".join(failed)
        report_str = f"""
        Extraction completed for model {model} in {total_time_pretty}.
        Successfully processed {images} images and {videos} videos,
        and {other} other file types.
        The model processed a total of {units} individual pieces of data.
        {len(failed)} files failed to process due to errors.
        """
        if len(failed) > 0:
            report_str += f"\nFailed files:\n{failed_str}"
    finally:
        conn.close()


@router.post(
    "/data/extraction/{group}/{inference_id}",
    summary="Run a data extraction job",
)
def data_extraction_job(
    background_tasks: BackgroundTasks,
    group: str = Path(..., title="Group name"),
    inference_id: str = Path(..., title="Inference ID"),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
    model_name = f"{group}/{inference_id}"
    from panoptikon.data_extractors.models import ModelOptsFactory

    model = ModelOptsFactory.get_model(model_name)
    background_tasks.add_task(
        run_data_extraction_job,
        model=model,
        conn_args=conn_args,
    )

def delete_model_data(model: ModelOpts,
    conn_args: Dict[str, Any],
):
    conn = get_database_connection(**conn_args)
    try:
        logger.info(f"Running data deletion job for model {model}")
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        report_str = model.delete_extracted_data(conn)
        conn.commit()
        vacuum_database(conn)
        return report_str
    finally:
        conn.close()

@router.delete(
    "/data/extraction/{group}/{inference_id}",
    summary="Delete extracted data",
)
def delete_extracted_data(
    background_tasks: BackgroundTasks,
    group: str = Path(..., title="Group name"),
    inference_id: str = Path(..., title="Inference ID"),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
    model_name = f"{group}/{inference_id}"
    from panoptikon.data_extractors.models import ModelOptsFactory

    model = ModelOptsFactory.get_model(model_name)
    background_tasks.add_task(
        delete_model_data,
        model=model,
        conn_args=conn_args,
    )

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

def rescan_folders(conn_args: Dict[str, Any]):
    conn = get_database_connection(**conn_args)
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        ids, files_deleted, items_deleted, rule_files_deleted = (
            rescan_all_folders(conn)
        )
        conn.commit()
        vacuum_database(conn)
        conn.close()
        return (
            f"Rescanned all folders. Removed {files_deleted} files and {items_deleted} orphaned items. "
            + f"Files deleted due to rules: {rule_files_deleted}"
        )
    finally:
        conn.close()


@router.post(
    "/folders/rescan",
    summary="Run a file scan",
)
def file_scan(
    background_tasks: BackgroundTasks,
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
    background_tasks.add_task(
        rescan_folders,
        conn_args=conn_args,
    )


def run_folder_update(
    included_folders: List[str],
    excluded_folders: List[str],
    conn_args: Dict[str, Any]
):
    new_included_folders = (
        [p.strip() for p in included_folders if len(p.strip()) > 0]
    )
    new_excluded_folders = (
        [p.strip() for p in excluded_folders if len(p.strip()) > 0]
    )
    conn = get_database_connection(**conn_args)
    try:
        cursor = conn.cursor()
        # Begin a transaction
        cursor.execute("BEGIN")
        update_result = update_folder_lists(
            conn,
            new_included_folders,
            new_excluded_folders,
        )
        update_result_text = f"""
        Included folders added (and scanned): {", ".join(update_result.included_added)} ({len(update_result.scan_ids)});
        """
        if update_result.excluded_added:
            update_result_text += f"\nExcluded folders added: {", ".join(update_result.excluded_added)};"
        if update_result.included_deleted or update_result.excluded_deleted:
            update_result_text += f"\nRemoved {update_result.included_deleted} included folders, {update_result.excluded_deleted} excluded folders;"
        if update_result.unavailable_files_deleted:
            update_result_text += f"\nRemoved {update_result.unavailable_files_deleted} files from the database which were no longer available on the filesystem;"
        if update_result.excluded_folder_files_deleted:
            update_result_text += f"\nRemoved {update_result.excluded_folder_files_deleted} files from the database that were inside excluded folders;"
        if update_result.orphan_files_deleted:
            update_result_text += f"\nRemoved {update_result.orphan_files_deleted} files from the database that were no longer inside included folders;"
        if update_result.rule_files_deleted:
            update_result_text += f"\nRemoved {update_result.rule_files_deleted} files from the database that were not allowed by user rules;"
        if update_result.orphan_items_deleted:
            update_result_text += f"\nRemoved {update_result.orphan_items_deleted} orphaned items (with no corresponding files) from the database. Any bookmarks on these items were also removed."
        conn.commit()
        vacuum_database(conn)
    except Exception as e:
        # Rollback the transaction on error
        conn.rollback()
        conn.close()
        return (
            f"Error: {e}",
            included_folders,
            excluded_folders,
        )
    conn.close()

    return (
        f"{update_result_text}",
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
    folders: Folders = Body(..., title="The new sets of included and excluded folders. Replaces the current lists with these."),
    conn_args: Dict[str, Any] = Depends(get_db_system_wl),
):
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
)-> Folders:
    conn = get_database_connection(**conn_args)
    try:
        current_included_folders = get_folders_from_database(conn, included=True)
        current_excluded_folders = get_folders_from_database(conn, included=False)
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
