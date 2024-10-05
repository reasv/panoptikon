import logging

from alembic import context
from sqlalchemy import create_engine

from panoptikon.db import get_db_lists, get_db_paths

logger = logging.getLogger(__name__)


def run_migrations_online():
    """Run migrations in 'online' mode using SQLAlchemy's create_engine."""

    db_file, default_user_db_file, storage_db_file = get_db_paths()
    db_files = [default_user_db_file]
    index_dbs, user_data_dbs = get_db_lists()
    for user_db in user_data_dbs:
        db_file, user_db_file, storage_db_file = get_db_paths(
            user_data_db=user_db
        )
        if user_db_file not in db_files:
            db_files.append(user_db_file)

    for user_db_file in db_files:
        # index_db_url = f"sqlite:///{db_file}"
        user_data_db_url = f"sqlite:///{user_db_file}"
        # storage_db_url = f"sqlite:///{storage_db_file}"
        # Create SQLAlchemy engines for the databases
        index_engine = create_engine(user_data_db_url)

        # Get a connection from the main database engine
        with index_engine.connect() as connection:
            # Set up Alembic's migration context
            context.configure(connection=connection, target_metadata=None)

            # Run the migrations within a transaction
            context.run_migrations()


# Alembic expects a function to call migrations either in offline or online mode.
if context.is_offline_mode():
    raise RuntimeError("Offline mode is not supported with raw SQLite.")
else:
    run_migrations_online()
