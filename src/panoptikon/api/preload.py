import logging
from datetime import datetime, timedelta

from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.config import retrieve_system_config
from panoptikon.data_extractors.models import get_inference_api_client
from panoptikon.db import get_database_connection
from panoptikon.db.extraction_log import get_existing_setters

logger = logging.getLogger(__name__)

# Store the next renewal time for each model in a dictionary
next_renewal_times = {}

preload_on = False


def preload_embedding_models(index_db: str, ttl: int = 3600):
    global next_renewal_times, preload_on
    system_config = retrieve_system_config(index_db)
    if not system_config.preload_embedding_models:
        if preload_on:
            logger.info("Disabling model preloading, clearing cache...")
            next_renewal_times = {}
            preload_on = False
            client = get_inference_api_client()
            client.clear_cache("preload")
        return
    preload_on = True
    conn = get_database_connection(**get_db_readonly(index_db, None))
    try:
        from panoptikon.data_extractors.models import ModelOptsFactory

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
            next_renewal_time = next_renewal_times.get(setter)
            if not next_renewal_time or current_time >= next_renewal_time:
                model = ModelOptsFactory.get_model(setter)
                model.load_model("preload", len(embedding_setters), ttl)
                next_renewal_times[setter] = current_time + timedelta(
                    seconds=ttl
                )
    except Exception as e:
        logger.error(f"Failed to preload embedding models: {e}", exc_info=True)
    finally:
        conn.close()
