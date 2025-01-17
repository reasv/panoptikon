import datetime
import logging
from typing import Any, Dict

from panoptikon.data_extractors.models import ModelOptsFactory
from panoptikon.data_extractors.types import (
    ExtractionJobProgress,
    ExtractionJobReport,
)
from panoptikon.db import ensure_close, get_database_connection, atomic_transaction
from panoptikon.db.extraction_log import (
    delete_data_job_by_log_id,
    remove_incomplete_jobs,
)
from panoptikon.db.utils import analyze_database, vacuum_database
from panoptikon.folders import (
    is_resync_needed,
    rescan_all_folders,
    update_folder_lists,
)

logger = logging.getLogger(__name__)


def run_folder_update(conn_args: Dict[str, Any]):
    from panoptikon.config import persist_system_config, retrieve_system_config

    with ensure_close(get_database_connection(**conn_args)) as conn:
        try:
            system_config = retrieve_system_config(conn_args["index_db"])
            # Ensures that paths are saved in the standard format
            persist_system_config(conn_args["index_db"], system_config)
            with atomic_transaction(conn, logger):
                update_result = update_folder_lists(conn, system_config)

            logger.info(
                f"""
            Included folders added (and scanned): {", ".join(update_result.included_added)} ({len(update_result.scan_ids)});
            """
            )
            data_deleted = False
            if update_result.excluded_added:
                logger.info(
                    f"Excluded folders added: {", ".join(update_result.excluded_added)};"
                )
            if update_result.included_deleted or update_result.excluded_deleted:
                logger.info(
                    f"Removed {update_result.included_deleted} included folders, {update_result.excluded_deleted} excluded folders;"
                )
            if update_result.unavailable_files_deleted:
                data_deleted = True
                logger.info(
                    f"Removed {update_result.unavailable_files_deleted} files from the database which were no longer available on the filesystem;"
                )
            if update_result.excluded_folder_files_deleted:
                data_deleted = True
                logger.info(
                    f"Removed {update_result.excluded_folder_files_deleted} files from the database that were inside excluded folders;"
                )
            if update_result.orphan_files_deleted:
                data_deleted = True
                logger.info(
                    f"Removed {update_result.orphan_files_deleted} files from the database that were no longer inside included folders;"
                )
            if update_result.rule_files_deleted:
                data_deleted = True
                logger.info(
                    f"Removed {update_result.rule_files_deleted} files from the database that were not allowed by user rules;"
                )
            if update_result.orphan_items_deleted:
                data_deleted = True
                logger.info(
                    f"Removed {update_result.orphan_items_deleted} orphaned items (with no corresponding files) from the database. Any bookmarks on these items were also removed."
                )
            if data_deleted:
                vacuum_database(conn)
            analyze_database(conn)
        except Exception as e:
            logger.error(f"Folder update failed with error: {e}")


def rescan_folders(conn_args: Dict[str, Any]):
    from panoptikon.config import retrieve_system_config

    with ensure_close(get_database_connection(**conn_args)) as conn:
        system_config = retrieve_system_config(conn_args["index_db"])
        resync_needed = is_resync_needed(conn, system_config)

    if resync_needed:
        logger.info("Resync needed, running folder update")
        run_folder_update(conn_args)

    with ensure_close(get_database_connection(**conn_args)) as conn:
        system_config = retrieve_system_config(conn_args["index_db"])
        if is_resync_needed(conn, system_config):
            logger.info("Resync needed, running folder update")
            run_folder_update(conn_args)

        with atomic_transaction(conn, logger):
            ids, files_deleted, items_deleted, rule_files_deleted = (
                rescan_all_folders(conn, system_config=system_config)
            )
        if files_deleted or items_deleted or rule_files_deleted:
            vacuum_database(conn)
        analyze_database(conn)
        logger.info(
            f"Rescanned all folders. Removed {files_deleted} files and {items_deleted} orphaned items. "
            + f"Files deleted due to rules: {rule_files_deleted}"
        )

def delete_model_data(
    inference_id: str,
    conn_args: Dict[str, Any],
):
    model = ModelOptsFactory.get_model(inference_id)
    with ensure_close(get_database_connection(**conn_args)) as conn:
        logger.info(f"Running data deletion job for model {model}")
        with atomic_transaction(conn, logger):
            report_str = model.delete_extracted_data(conn)
            logger.info(report_str)
        vacuum_database(conn)
        analyze_database(conn)


def delete_job_data(
    log_id: int,
    conn_args: Dict[str, Any],
):
    with ensure_close(get_database_connection(**conn_args)) as conn:
        logger.info(f"Running data deletion job log id {log_id}")
        with atomic_transaction(conn, logger):
            delete_data_job_by_log_id(conn, log_id)
            logger.info(f"Deleted data for job log id {log_id}")
        vacuum_database(conn)
        analyze_database(conn)

def run_data_extraction_job(
    inference_id: str,
    batch_size: int | None,
    threshold: float | None,
    conn_args: Dict[str, Any],
):
    from panoptikon.config import retrieve_system_config
    with ensure_close(get_database_connection(**conn_args)) as conn:
        system_config = retrieve_system_config(conn_args["index_db"])
        resync_needed = is_resync_needed(conn, system_config)

    if resync_needed:
        logger.info(
            "Folders in config changed. Resync needed, running folder update"
        )
        run_folder_update(conn_args)

    model = ModelOptsFactory.get_model(inference_id)
    with ensure_close(get_database_connection(**conn_args)) as conn:
        try:
            with atomic_transaction(conn, logger):
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
            analyze_database(conn)
        except Exception as e:
            logger.error(
                f"Data extraction job for model {model} failed with error: {e}",
                exc_info=True,
            )
            with atomic_transaction(conn, logger):
                remove_incomplete_jobs(conn)
                logger.info("Removed incomplete jobs from the database")