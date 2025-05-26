import subprocess
import types
from typing import Optional, Any
import logging
import concurrent.futures
from collections import defaultdict

from sqlalchemy import Engine, Table
from sqlalchemy.orm import Session
from typing_extensions import Self

from .config import AppSettings, MaskingConfig
from .db import DBManager, create_db_manager
from .ssh import SSHTunnelManager, create_ssh_tunnel
from .utils import (
    apply_masking,
    chunk_iterable,
    get_alembic_version,
    get_db_config_for_connection,
    get_sorted_tables,
)

logger = logging.getLogger(__name__)


# Helper function to process a single table
def _process_table_data(
    table: Table,
    from_db_manager: DBManager,
    to_db_manager: DBManager,
    chunk_size: int,
    masking_settings: Optional[MaskingConfig],
) -> str:
    """
    Fetches data from the source table, applies masking if configured,
    and inserts it into the target table.
    Handles its own session and transaction.
    """
    logger.info(f"Starting data migration for table: {table.name}")
    from_session = from_db_manager.get_session()
    to_session = to_db_manager.get_session()
    try:
        rows_processed_count = 0
        # Iterate over data in chunks directly from the source query
        source_query = from_session.execute(table.select())
        
        for chunk_of_keys in chunk_iterable(source_query.keys(), chunk_size): # Process by smaller key chunks if row is too large
            # Re-fetch rows for the current chunk of keys to keep memory usage down
            # This is a conceptual placeholder; actual chunking needs to be on rows, not keys.
            # The original code fetches all rows then chunks. We'll stick to that for now and optimize if needed.
            # For now, let's assume the original chunking of all rows is acceptable.

            # The original code fetched all rows, then chunked. We will replicate that for now.
            # If memory becomes an issue, we'd need to stream rows and chunk them.
            pass # Placeholder, original logic is below.

        # Fetch all rows for the table (as per original logic)
        all_rows = list(from_session.execute(table.select()).mappings().all())
        logger.debug(f"Fetched {len(all_rows)} rows from source table {table.name}.")

        for chunk in chunk_iterable(all_rows, chunk_size):
            logger.debug(f"Processing chunk of {len(chunk)} rows for table {table.name} in target DB.")
            processed_chunk = []
            for row_data in chunk:
                row_dict = dict(row_data)
                if masking_settings and masking_settings.rules and table.name in masking_settings.rules:
                    logger.debug(f"Applying masking for table {table.name}, row: {row_dict}")
                    processed_chunk.append(
                        apply_masking(
                            row_dict,
                            table.name,
                            masking_settings.rules,
                        )
                    )
                else:
                    processed_chunk.append(row_dict)
            
            if processed_chunk:
                to_session.execute(table.insert(), processed_chunk)
            rows_processed_count += len(processed_chunk)

        to_session.commit()
        logger.info(f"Successfully migrated {rows_processed_count} rows for table: {table.name} and committed to target DB.")
        return table.name
    except Exception as e:
        logger.exception(f"Error processing table {table.name}. Rolling back transaction for this table.")
        if to_session:
            to_session.rollback()
        # Re-raise the exception to be caught by the main executor loop
        raise Exception(f"Failed to process table {table.name}: {e}") from e
    finally:
        if from_session:
            from_session.close()
        if to_session:
            to_session.close()


def run_alembic_cmd(
    alembic_dir: str, db_url: str, cmd: str, revision: str = ""
) -> None:
    """Run alembic CLI command (e.g., downgrade/upgrade)."""
    args = [
        "alembic",
        "-c",
        f"{alembic_dir}/alembic.ini",
        "-x",
        f"db_url={db_url}",
        cmd,
    ]
    if revision:
        args.append(revision)
    logger.info(f"Running Alembic command: {' '.join(args)}")
    subprocess.run(args, check=True)
    logger.info("Alembic command completed successfully.")


def sync_schema(from_db: Engine, to_db: Engine, alembic_dir: str) -> None:
    """Sync target DB schema to source DB revision."""
    logger.info(f"Starting schema sync for target DB: {to_db.engine.url.database}")
    from_rev = get_alembic_version(from_db)
    if from_rev is None:
        raise ValueError("Cannot find alembic revision in source_db.")

    to_db_url = to_db.url.render_as_string(hide_password=False)
    target_current_rev = get_alembic_version(to_db)
    logger.info(f"Source DB Alembic revision: {from_rev}, Target DB current revision: {target_current_rev}")

    if target_current_rev is None:
        logger.info("Target DB has no revision, upgrading to source DB revision.")
        run_alembic_cmd(alembic_dir, to_db_url, "upgrade", from_rev)
        return
    logger.info("Downgrading target DB to base, then upgrading to source DB revision.")
    run_alembic_cmd(alembic_dir, to_db_url, "downgrade", "base")
    run_alembic_cmd(alembic_dir, to_db_url, "upgrade", from_rev)


def dump_and_load(settings: AppSettings, alembic_dir: str) -> None:
    """Main data migration workflow."""
    logger.info("Starting dump and load process.")
    # SSH Tunneling (if needed)
    from_ctx: Optional[SSHTunnelManager] = None
    if settings.source_ssh_tunnel:
        logger.info("SSH tunnel configured for source DB.")
        from_ctx = create_ssh_tunnel(settings.source_ssh_tunnel, settings.source_db)

    to_ctx: Optional[SSHTunnelManager] = None
    if settings.target_ssh_tunnel:
        logger.info("SSH tunnel configured for target DB.")
        to_ctx = create_ssh_tunnel(settings.target_ssh_tunnel, settings.target_db)

    with (
        from_ctx.tunnel() if from_ctx else nullcontext() as active_from_tunnel, # type: ignore
        to_ctx.tunnel() if to_ctx else nullcontext() as active_to_tunnel, # type: ignore
    ):
        if from_ctx and active_from_tunnel: # active_from_tunnel could be None if nullcontext()
            logger.info(f"SSH tunnel for source DB active: {getattr(active_from_tunnel, 'local_bind_address', 'N/A')}")
        if to_ctx and active_to_tunnel: # active_to_tunnel could be None if nullcontext()
            logger.info(f"SSH tunnel for target DB active: {getattr(active_to_tunnel, 'local_bind_address', 'N/A')}")

        source_db_config = get_db_config_for_connection(
            settings.source_db,
            active_from_tunnel # type: ignore
            if isinstance(active_from_tunnel, SSHTunnelManager) 
            else None,
        )
        target_db_config = get_db_config_for_connection(
            settings.target_db,
            active_to_tunnel # type: ignore
            if isinstance(active_to_tunnel, SSHTunnelManager)
            else None,
        )

        with (
            create_db_manager(source_db_config) as from_db,
            create_db_manager(target_db_config) as to_db,
        ):
            logger.info(f"Source DB manager created for: {source_db_config.database}, Target DB manager created for: {target_db_config.database}")
            # Schema synchronization
            logger.info(f"Initiating schema sync between source and target on alembic_dir: {alembic_dir}.")
            sync_schema(from_db.engine, to_db.engine, alembic_dir)
            logger.info("Schema synchronization complete.")

            # Get tables grouped by dependency levels
            # get_sorted_tables now returns list[list[Table]]
            all_table_groups = get_sorted_tables(from_db.get_metadata()) 
            
            # Apply include/exclude filters to these groups
            tables_to_exclude_names = set(settings.tables_to_exclude or [])
            tables_to_include_names = set(settings.tables_to_include or []) # Empty means include all not excluded

            final_table_groups = []
            num_total_tables_after_filtering = 0
            for i, group in enumerate(all_table_groups):
                filtered_group = []
                for table in group:
                    if table.name in tables_to_exclude_names:
                        logger.debug(f"Table '{table.name}' in group {i} excluded by exclude list.")
                        continue
                    if tables_to_include_names and table.name not in tables_to_include_names:
                        logger.debug(f"Table '{table.name}' in group {i} excluded by include list (not present).")
                        continue
                    filtered_group.append(table)
                
                if filtered_group:
                    final_table_groups.append(filtered_group)
                    num_total_tables_after_filtering += len(filtered_group)
            
            logger.info(f"Found {num_total_tables_after_filtering} tables to migrate in {len(final_table_groups)} dependency groups "
                        f"(Include list: {settings.tables_to_include}, Exclude list: {settings.tables_to_exclude}).")

            overall_success = True
            # Process tables group by group, in parallel within each group
            for i, group_of_tables in enumerate(final_table_groups):
                if not group_of_tables: # Should be handled by the filtering logic above
                    continue

                # max_workers is the number of tables in the current parallelizable group
                max_group_workers = len(group_of_tables)
                
                group_table_names = [t.name for t in group_of_tables]
                logger.info(f"Processing dependency group {i+1}/{len(final_table_groups)} with {len(group_of_tables)} table(s): {group_table_names}. Max workers for this group: {max_group_workers}")

                with concurrent.futures.ThreadPoolExecutor(max_workers=max_group_workers) as executor:
                    future_to_table = {
                        executor.submit(
                            _process_table_data,
                            table,
                            from_db, # Pass DBManager instances
                            to_db,   # Pass DBManager instances
                            settings.chunk_size,
                            settings.masking,
                        ): table # Store the table object itself for error reporting
                        for table in group_of_tables
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_table):
                        table_obj = future_to_table[future]
                        table_name = table_obj.name
                        try:
                            result = future.result() # result is table_name on success
                            logger.info(f"Successfully completed migration for table: {result} (from group {i+1}).")
                        except Exception as exc:
                            logger.error(f"Table migration failed for '{table_name}' in group {i+1}: {exc}", exc_info=True)
                            overall_success = False
                            # Critical decision: Stop all further processing on first failure.
                            # Cancel remaining futures in the current group.
                            for f, t_obj in future_to_table.items():
                                if not f.done():
                                    logger.info(f"Cancelling task for table: {t_obj.name}")
                                    f.cancel()
                            logger.error(f"Aborting remaining tasks in group {i+1} due to error in table {table_name}.")
                            break  # Exit as_completed loop for the current group
                
                if not overall_success:
                    logger.error(f"Aborting dump and load process due to error in dependency group {i+1}.")
                    break # Exit the loop over dependency groups

            if overall_success:
                logger.info("All tables migrated successfully.")
            else:
                logger.error("Dump and load process failed due to errors in table migration.")
                # No global rollback needed here as each table handles its own transaction.
                # Raising an exception will indicate failure to the caller.
                raise Exception("Data migration failed for one or more tables.")

    logger.info("Dump and load process completed.")


try:
    from contextlib import nullcontext # type: ignore Python 3.7+
except ImportError:
    # For Python 3.6
    class nullcontext(object): # type: ignore
        def __init__(self, enter_result: Any = None) -> None:
            self.enter_result = enter_result

        def __enter__(self) -> Any:
            return self.enter_result

        def __exit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[types.TracebackType],
        ) -> None:
            pass
