import pytest
import psycopg2
import os
import subprocess
import logging
from datetime import date
from unittest.mock import patch # For simulating sequential processing

from sqlalchemy import create_engine, text, inspect

from alembic_dump.config import AppSettings, DBConfig
from alembic_dump.core import dump_and_load

logger = logging.getLogger(__name__)

# List of all tables used in these tests, in a plausible dependency order for verification
ALL_TABLES_TO_CHECK = ["categories", "users", "products", "orders", "order_items"]

# Helper to apply migrations, adapted from existing test_workflow.py
def _apply_migrations_for_parallel(
    db_url: str, alembic_env_path: str, revision: str = "head"
):
    logger.info(
        f"Applying Alembic migrations to {db_url.split('@')[-1]} up to revision '{revision}' using ini from '{alembic_env_path}'"
    )
    try:
        subprocess.run(
            [
                "alembic",
                "-c",
                os.path.join(alembic_env_path, "alembic.ini"),
                "-x",
                f"db_url={db_url}",
                "upgrade",
                revision,
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        logger.info(
            f"Successfully applied Alembic migrations to {db_url.split('@')[-1]}."
        )
    except subprocess.CalledProcessError as e:
        logger.error(
            f"Failed to apply Alembic migrations to {db_url.split('@')[-1]}. Revision: {revision}"
        )
        logger.error(f"Alembic command: {' '.join(e.cmd)}")
        logger.error(f"Return code: {e.returncode}")
        if e.stdout:
            logger.error(f"Alembic stdout:\n{e.stdout}")
        if e.stderr:
            logger.error(f"Alembic stderr:\n{e.stderr}")
        raise

def _get_db_connection_params(container_info: dict) -> dict:
    """Extracts psycopg2 connection parameters from container fixture info."""
    return {
        "host": container_info["host_for_host_machine"],
        "port": container_info["port_on_host"],
        "user": container_info["user"],
        "password": container_info["password"],
        "dbname": container_info["database"],
    }

def _populate_all_data(pg_conn_params: dict):
    with psycopg2.connect(**pg_conn_params) as conn:
        with conn.cursor() as cur:
            # Clear existing data (optional, good for reruns)
            for table in reversed(ALL_TABLES_TO_CHECK): # Delete from child tables first
                cur.execute(f"DELETE FROM {table};")

            # Categories
            cur.execute("INSERT INTO categories (id, name) VALUES (%s, %s);", [(1, "Electronics"), (2, "Books")])
            # Users
            cur.execute("INSERT INTO users (id, name, email) VALUES (%s, %s, %s);", [(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com"), (3, "Charlie", "charlie@example.com")])
            # Products
            cur.execute("INSERT INTO products (id, name, category_id) VALUES (%s, %s, %s);", [
                (1, "Laptop", 1), (2, "Python Programming Book", 2), (3, "Keyboard", 1), (4, "Database Design Book", 2)
            ])
            # Orders
            cur.execute("INSERT INTO orders (id, user_id, order_date) VALUES (%s, %s, %s);", [
                (1, 1, date(2023, 1, 15)), (2, 2, date(2023, 2, 10)), (3, 1, date(2023, 3, 5))
            ])
            # Order Items
            cur.execute("INSERT INTO order_items (id, order_id, product_id, quantity) VALUES (%s, %s, %s, %s);", [
                (1, 1, 1, 1), (2, 1, 2, 2), (3, 2, 3, 1), (4, 3, 4, 1), (5, 3, 1, 1)
            ])
        conn.commit()
    logger.info(f"Populated data in database: {pg_conn_params['dbname']}")

def _verify_data_consistency(source_pg_conn_params: dict, target_pg_conn_params: dict):
    source_url = f"postgresql://{source_pg_conn_params['user']}:{source_pg_conn_params['password']}@{source_pg_conn_params['host']}:{source_pg_conn_params['port']}/{source_pg_conn_params['dbname']}"
    target_url = f"postgresql://{target_pg_conn_params['user']}:{target_pg_conn_params['password']}@{target_pg_conn_params['host']}:{target_pg_conn_params['port']}/{target_pg_conn_params['dbname']}"
    
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)

    insp_source = inspect(source_engine)
    insp_target = inspect(target_engine)

    for table_name in ALL_TABLES_TO_CHECK:
        assert insp_source.has_table(table_name), f"Source DB missing table {table_name}"
        assert insp_target.has_table(table_name), f"Target DB missing table {table_name}"

        with source_engine.connect() as s_conn, target_engine.connect() as t_conn:
            pk_column = "id" # Common assumption for these tests

            source_data = s_conn.execute(text(f"SELECT * FROM {table_name} ORDER BY {pk_column}")).fetchall()
            target_data = t_conn.execute(text(f"SELECT * FROM {table_name} ORDER BY {pk_column}")).fetchall()

            assert len(source_data) == len(target_data), \
                f"Row count mismatch for table {table_name}. Source: {len(source_data)}, Target: {len(target_data)}"
            
            for i, (s_row, t_row) in enumerate(zip(source_data, target_data)):
                assert s_row == t_row, \
                    f"Data mismatch in table {table_name} at row {i} (ordered by {pk_column}).\nSource: {s_row}\nTarget: {t_row}"
        logger.info(f"Data for table {table_name} verified successfully. Row count: {len(source_data)}")
    
    source_engine.dispose()
    target_engine.dispose()

def _run_migration_scenario(
    source_pg_container_fx, 
    target_pg_container_fx, 
    alembic_test_env_dir_fx, 
    app_settings_overrides: dict = None,
    mock_executor_sequentially: bool = False
):
    source_conn_params = _get_db_connection_params(source_pg_container_fx)
    target_conn_params = _get_db_connection_params(target_pg_container_fx)

    _apply_migrations_for_parallel(source_pg_container_fx["sqlalchemy_url_on_host"], alembic_test_env_dir_fx, "0003")
    _apply_migrations_for_parallel(target_pg_container_fx["sqlalchemy_url_on_host"], alembic_test_env_dir_fx, "0003")
    
    _populate_all_data(source_conn_params)

    source_db_app_config = DBConfig(
        driver="postgresql", host=source_conn_params["host"], port=source_conn_params["port"],
        username=source_conn_params["user"], password=source_conn_params["password"], database=source_conn_params["dbname"]
    )
    target_db_app_config = DBConfig(
        driver="postgresql", host=target_conn_params["host"], port=target_conn_params["port"],
        username=target_conn_params["user"], password=target_conn_params["password"], database=target_conn_params["dbname"]
    )

    # Base settings, no longer includes table_priorities or max_parallel_tasks
    settings_dict = {
        "source_db": source_db_app_config,
        "target_db": target_db_app_config,
        "chunk_size": 5, 
        "masking": None,
        "tables_to_exclude": ["alembic_version"],
        "source_ssh_tunnel": None,
        "target_ssh_tunnel": None,
    }
    if app_settings_overrides: # e.g. for testing different chunk_size
        settings_dict.update(app_settings_overrides)
        
    settings = AppSettings(**settings_dict)
    
    logger.info(f"Running dump_and_load with settings: {settings.model_dump_json(indent=2)}")

    if mock_executor_sequentially:
        # Mock ThreadPoolExecutor to run tasks sequentially for this test
        with patch("concurrent.futures.ThreadPoolExecutor") as mock_executor_cls:
            mock_executor_instance = MagicMock()
            # Make submit call the function immediately and return a future that has already resolved
            def sequential_submit(fn, *args, **kwargs):
                try:
                    result = fn(*args, **kwargs)
                    future = MagicMock()
                    future.result.return_value = result
                    return future
                except Exception as e:
                    future = MagicMock()
                    future.result.side_effect = e
                    return future

            mock_executor_instance.submit.side_effect = sequential_submit
            # as_completed should yield these 'resolved' futures
            mock_executor_instance.map = lambda fn_map, iter_map, *a, **kwa: map(fn_map, iter_map) # simplified map
            
            def sequential_as_completed(fs):
                return fs # Yield futures as they are, they are already "resolved" by sequential_submit

            mock_executor_cls.return_value.__enter__.return_value = mock_executor_instance
            with patch("concurrent.futures.as_completed", side_effect=sequential_as_completed):
                 dump_and_load(settings, alembic_test_env_dir_fx)
    else:
        dump_and_load(settings, alembic_test_env_dir_fx)
        
    _verify_data_consistency(source_conn_params, target_conn_params)

# --- Test Scenarios ---

@pytest.mark.integration
def test_migration_automatic_parallelism(source_pg_container, target_pg_container, alembic_test_env_dir):
    """
    Tests end-to-end data migration relying on automatic dependency grouping and inherent parallelism.
    Verifies data integrity after migration.
    """
    logger.info("=== Test Scenario: Automatic Parallelism (End-to-End) ===")
    _run_migration_scenario(source_pg_container, target_pg_container, alembic_test_env_dir)

@pytest.mark.integration
def test_migration_simulated_sequential_processing(source_pg_container, target_pg_container, alembic_test_env_dir):
    """
    Tests end-to-end data migration with ThreadPoolExecutor mocked to run tasks sequentially.
    This verifies data integrity even if parallelism is effectively disabled or limited to 1.
    """
    logger.info("=== Test Scenario: Simulated Sequential Processing (End-to-End) ===")
    _run_migration_scenario(
        source_pg_container, 
        target_pg_container, 
        alembic_test_env_dir,
        mock_executor_sequentially=True
    )

@pytest.mark.integration
def test_migration_larger_chunk_size(source_pg_container, target_pg_container, alembic_test_env_dir):
    """
    Tests end-to-end data migration with a larger chunk_size to ensure it handles
    different batching scenarios correctly with automatic parallelism.
    """
    logger.info("=== Test Scenario: Larger Chunk Size with Automatic Parallelism ===")
    _run_migration_scenario(
        source_pg_container, 
        target_pg_container, 
        alembic_test_env_dir,
        app_settings_overrides={"chunk_size": 50} # Default test chunk is 5, make this larger
    )

# Add more tests here if specific data conditions or schema aspects need verification
# under the automatic parallelism model. For example:
# - A test with one table having significantly more rows than others.
# - A test with tables that have many FK dependencies vs. mostly independent tables.
# The current schema (ALL_TABLES_TO_CHECK) provides a good mix already.
