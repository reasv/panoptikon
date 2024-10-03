import datetime
import logging
from typing import Any, Dict, List

from panoptikon.data_extractors.extraction_jobs.types import (
    ExtractionJobProgress,
    ExtractionJobReport,
)
from panoptikon.data_extractors.models import ModelOpts
from panoptikon.db import get_database_connection
from panoptikon.db.utils import vacuum_database
from panoptikon.folders import rescan_all_folders, update_folder_lists

logger = logging.getLogger(__name__)

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
        logger.info(f"""
        Included folders added (and scanned): {", ".join(update_result.included_added)} ({len(update_result.scan_ids)});
        """)
        if update_result.excluded_added:
            logger.info(f"Excluded folders added: {", ".join(update_result.excluded_added)};")
        if update_result.included_deleted or update_result.excluded_deleted:
            logger.info(f"Removed {update_result.included_deleted} included folders, {update_result.excluded_deleted} excluded folders;")
        if update_result.unavailable_files_deleted:
            logger.info(f"Removed {update_result.unavailable_files_deleted} files from the database which were no longer available on the filesystem;")
        if update_result.excluded_folder_files_deleted:
            logger.info(f"Removed {update_result.excluded_folder_files_deleted} files from the database that were inside excluded folders;")
        if update_result.orphan_files_deleted:
            logger.info(f"Removed {update_result.orphan_files_deleted} files from the database that were no longer inside included folders;")
        if update_result.rule_files_deleted:
            logger.info(f"Removed {update_result.rule_files_deleted} files from the database that were not allowed by user rules;")
        if update_result.orphan_items_deleted:
            logger.info(f"Removed {update_result.orphan_items_deleted} orphaned items (with no corresponding files) from the database. Any bookmarks on these items were also removed.")
        
        conn.commit()
        vacuum_database(conn)
    except Exception as e:
        # Rollback the transaction on error
        conn.rollback()
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

def delete_model_data(
    model: ModelOpts,
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
