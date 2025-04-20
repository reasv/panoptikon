import logging
from datetime import datetime, timedelta

from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.config import retrieve_system_config
from panoptikon.data_extractors.models import get_inference_api_client
from panoptikon.db import ensure_close, get_database_connection
from panoptikon.db.extraction_log import get_existing_setters

logger = logging.getLogger(__name__)

next_renewal_times = {}

preload_status = {}


def preload_embedding_models(index_db: str, ttl: int = 3600):
    global next_renewal_times, preload_status

    # Initialize the next_renewal_times and preload_status for this index_db if not already present
    if index_db not in next_renewal_times:
        next_renewal_times[index_db] = {}
    if index_db not in preload_status:
        preload_status[index_db] = False

    system_config = retrieve_system_config(index_db)

    if not system_config.preload_embedding_models:
        # Preload was previously enabled, but now it is disabled, so clear the cache
        if preload_status[index_db]:
            logger.info(
                f"Disabling model preloading for {index_db}, clearing cache..."
            )
            next_renewal_times[index_db] = (
                {}
            )  # Clear renewal times for this index_db
            preload_status[index_db] = False
            client = get_inference_api_client()
            client.clear_cache(f"preload[{index_db}]")
        return

    preload_status[index_db] = True
    with ensure_close(get_database_connection(**get_db_readonly(index_db, None))) as conn:
        try:
            from panoptikon.data_extractors.models import load_model

            embedding_setters = [
                setter
                for datatype, setter in get_existing_setters(conn)
                if (
                    datatype in ["text-embedding", "clip"]
                    and (not setter.startswith("tclip/"))
                )
            ]
            if not embedding_setters:
                return

            current_time = datetime.now()
            for setter in embedding_setters:
                next_renewal_time = next_renewal_times[index_db].get(setter)
                if not next_renewal_time or current_time >= next_renewal_time:
                    try:
                       load_model(setter, f"preload[{index_db}]", len(embedding_setters), ttl)
                    except Exception as e:
                        logger.error(
                            f"Failed to preload embedding model for {setter}: {e}")
                        continue
                    next_renewal_times[index_db][setter] = current_time + timedelta(
                        seconds=max(ttl - 130, 60)  # Renew >2 minutes before expiry
                    )
        except Exception as e:
            logger.error(
                f"Failed to preload embedding models for {index_db}: {e}",
                exc_info=True,
            )
