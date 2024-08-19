from alembic import context

from panoptikon.db import get_database_connection

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config


def run_migrations_online():
    """Run migrations in 'online' mode with sqlite3 connection."""

    # Get your sqlite3 connection using your existing function
    connection = get_database_connection(write_lock=True, user_data_wl=True)

    # Create an Alembic context from the sqlite3 connection
    context.configure(connection=connection, target_metadata=None)  # type: ignore

    # Begin a migration transaction
    with connection:
        with context.begin_transaction():
            context.run_migrations()


# Alembic expects a function to call migrations either in offline or online mode.
if context.is_offline_mode():
    raise RuntimeError("Offline mode is not supported with raw SQLite.")
else:
    run_migrations_online()
