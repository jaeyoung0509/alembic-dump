import os
import sys

print("Alembic env.py sys.path:", sys.path)
print("PYTHONPATH env var:", os.environ.get("PYTHONPATH"))  # os 모듈 import 필요

from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData, engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = MetaData()


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Get the database URL from the -x db_url=... command line option
    # or from the alembic.ini file if not provided via -x
    db_url = context.get_x_argument(as_dictionary=True).get("db_url")
    if not db_url:
        db_url = config.get_main_option(
            "sqlalchemy.url"
        )  # Fallback, though test should provide it via -x

    connectable = engine_from_config(
        {"sqlalchemy.url": db_url},  # Use the URL from -x
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
