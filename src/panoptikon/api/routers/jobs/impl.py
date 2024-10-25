import datetime
import logging
from typing import Any, Dict

from panoptikon.config import persist_system_config, retrieve_system_config
from panoptikon.data_extractors.extraction_jobs.types import (
    ExtractionJobProgress,
    ExtractionJobReport,
)
from panoptikon.data_extractors.models import ModelOptsFactory
from panoptikon.db import get_database_connection
from panoptikon.db.extraction_log import (
    delete_data_job_by_log_id,
    remove_incomplete_jobs,
)
from panoptikon.db.utils import vacuum_database
from panoptikon.folders import (
    is_resync_needed,
    rescan_all_folders,
    update_folder_lists,
)

logger = logging.getLogger(__name__)


def run_folder_update(conn_args: Dict[str, Any]):
    conn = get_database_connection(**conn_args)
    try:
        system_config = retrieve_system_config(conn_args["index_db"])
        # Ensures that paths are saved in the standard format
        persist_system_config(conn_args["index_db"], system_config)
        cursor = conn.cursor()
        # Begin a transaction
        cursor.execute("BEGIN")
        update_result = update_folder_lists(conn, system_config)
        logger.info(
            f"""
        Included folders added (and scanned): {", ".join(update_result.included_added)} ({len(update_result.scan_ids)});
        """
        )
        if update_result.excluded_added:
            logger.info(
                f"Excluded folders added: {", ".join(update_result.excluded_added)};"
            )
        if update_result.included_deleted or update_result.excluded_deleted:
            logger.info(
                f"Removed {update_result.included_deleted} included folders, {update_result.excluded_deleted} excluded folders;"
            )
        if update_result.unavailable_files_deleted:
            logger.info(
                f"Removed {update_result.unavailable_files_deleted} files from the database which were no longer available on the filesystem;"
            )
        if update_result.excluded_folder_files_deleted:
            logger.info(
                f"Removed {update_result.excluded_folder_files_deleted} files from the database that were inside excluded folders;"
            )
        if update_result.orphan_files_deleted:
            logger.info(
                f"Removed {update_result.orphan_files_deleted} files from the database that were no longer inside included folders;"
            )
        if update_result.rule_files_deleted:
            logger.info(
                f"Removed {update_result.rule_files_deleted} files from the database that were not allowed by user rules;"
            )
        if update_result.orphan_items_deleted:
            logger.info(
                f"Removed {update_result.orphan_items_deleted} orphaned items (with no corresponding files) from the database. Any bookmarks on these items were also removed."
            )

        conn.commit()
        vacuum_database(conn)
    except Exception as e:
        # Rollback the transaction on error
        conn.rollback()
        logger.error(f"Folder update failed with error: {e}")
    conn.close()


def rescan_folders(conn_args: Dict[str, Any]):
    conn = get_database_connection(**conn_args)
    try:
        system_config = retrieve_system_config(conn_args["index_db"])
        resync_needed = is_resync_needed(conn, system_config)
    finally:
        conn.close()
    if resync_needed:
        logger.info("Resync needed, running folder update")
        run_folder_update(conn_args)

    conn = get_database_connection(**conn_args)
    try:
        system_config = retrieve_system_config(conn_args["index_db"])
        if is_resync_needed(conn, system_config):
            logger.info("Resync needed, running folder update")
            run_folder_update(conn_args)

        cursor = conn.cursor()
        cursor.execute("BEGIN")
        ids, files_deleted, items_deleted, rule_files_deleted = (
            rescan_all_folders(conn, system_config=system_config)
        )
        conn.commit()
        vacuum_database(conn)
        conn.close()
        logger.info(
            f"Rescanned all folders. Removed {files_deleted} files and {items_deleted} orphaned items. "
            + f"Files deleted due to rules: {rule_files_deleted}"
        )
    finally:
        conn.close()


def delete_model_data(
    inference_id: str,
    conn_args: Dict[str, Any],
):
    model = ModelOptsFactory.get_model(inference_id)
    conn = get_database_connection(**conn_args)
    try:
        logger.info(f"Running data deletion job for model {model}")
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        report_str = model.delete_extracted_data(conn)
        logger.info(report_str)
        conn.commit()
        vacuum_database(conn)
    finally:
        conn.close()


def delete_job_data(
    log_id: int,
    conn_args: Dict[str, Any],
):
    conn = get_database_connection(**conn_args)
    try:
        logger.info(f"Running data deletion job log id {log_id}")
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        delete_data_job_by_log_id(conn, log_id)
        logger.info(f"Deleted data for job log id {log_id}")
        conn.commit()
        vacuum_database(conn)
    finally:
        conn.close()


def run_data_extraction_job(
    inference_id: str,
    batch_size: int | None,
    threshold: float | None,
    conn_args: Dict[str, Any],
):
    conn = get_database_connection(**conn_args)
    try:
        system_config = retrieve_system_config(conn_args["index_db"])
        resync_needed = is_resync_needed(conn, system_config)
    finally:
        conn.close()
    if resync_needed:
        logger.info(
            "Folders in config changed. Resync needed, running folder update"
        )
        run_folder_update(conn_args)

    model = ModelOptsFactory.get_model(inference_id)
    conn = get_database_connection(**conn_args)
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        failed, images, videos, other, units = [], 0, 0, 0, 0
        start_time = datetime.datetime.now()
        for progress in model.run_extractor(
            conn, system_config, batch_size, threshold
        ):
            if type(progress) == ExtractionJobProgress:
                # Job is in progress
                pass
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
        failed_str = ", ".join(failed)
        logger.info(
            f"""
        Extraction completed for model {model} in {total_time_pretty}.
        Successfully processed {images} images and {videos} videos,
        and {other} other file types.
        The model processed a total of {units} individual pieces of data.
        {len(failed)} files failed to process due to errors.
        """
        )
        if len(failed) > 0:
            logger.info(f"Failed files: {failed_str}")
        logger.info(f"Running vacuum and analyze on database")
        vacuum_database(conn)
        conn.commit()
    except Exception as e:
        logger.error(
            f"Data extraction job for model {model} failed with error: {e}",
            exc_info=True,
        )
        conn.rollback()
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        remove_incomplete_jobs(conn)
        conn.commit()
        logger.info("Removed incomplete jobs from the database")
    finally:
        conn.close()
