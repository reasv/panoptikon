import logging

from alembic import context
from sqlalchemy import create_engine

from panoptikon.db import get_db_paths

logger = logging.getLogger(__name__)


def run_migrations_online():
    """Run migrations in 'online' mode using SQLAlchemy's create_engine."""

    db_file, user_db_file, storage_db_file = get_db_paths()
    index_db_url = f"sqlite:///{db_file}"
    # user_data_db_url = f"sqlite:///{user_db_file}"
    # storage_db_url = f"sqlite:///{storage_db_file}"
    # Create SQLAlchemy engines for the databases
    index_engine = create_engine(index_db_url)

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
