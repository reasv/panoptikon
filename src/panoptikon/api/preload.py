import logging

from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.config import retrieve_system_config
from panoptikon.db import get_database_connection
from panoptikon.db.extraction_log import get_existing_setters

logger = logging.getLogger(__name__)


def preload_embedding_models(index_db: str):
    system_config = retrieve_system_config(index_db)
    if not system_config.preload_embedding_models:
        return
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
        for setter in embedding_setters:
            model = ModelOptsFactory.get_model(setter)
            model.load_model("preload", len(embedding_setters), 180)
    except Exception as e:
        logger.error(f"Failed to preload embedding models: {e}", exc_info=True)
    finally:
        conn.close()
