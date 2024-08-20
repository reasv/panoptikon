from typing import Optional

from fastapi import Query

from panoptikon.db import get_database_connection


def get_db_readonly(
    index_db: Optional[str] = Query(None),
    user_data_db: Optional[str] = Query(None),
):
    return get_database_connection(
        write_lock=False, index_db=index_db, user_data_db=user_data_db
    )


def get_db_user_data_wl(
    index_db: Optional[str] = Query(None),
    user_data_db: Optional[str] = Query(None),
):
    return get_database_connection(
        write_lock=False,
        user_data_wl=True,
        index_db=index_db,
        user_data_db=user_data_db,
    )
