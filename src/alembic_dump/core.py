import subprocess
import types
from typing import Optional
import logging
import concurrent.futures
import os

from sqlalchemy import Engine, Table # Added Table for type hinting
from typing_extensions import Self

from .config import AppSettings
from .db import DBManager, create_db_manager # Added DBManager for type hinting
from .ssh import SSHTunnelManager, create_ssh_tunnel
from .utils import (
    apply_masking,
    chunk_iterable,
    get_alembic_version,
    get_db_config_for_connection,
    get_parallel_execution_groups, # Changed from get_sorted_tables
)

logger = logging.getLogger(__name__)


def _process_table_data(table: Table, source_db_manager: DBManager, target_db_manager: DBManager, settings: AppSettings, alembic_dir: str): # alembic_dir kept as per instruction
    logger.info(f"Starting data processing for table: {table.name}")
    try:
        # Each thread needs its own session
        with source_db_manager.get_session() as from_session, target_db_manager.get_session() as to_session:
            rows = list(
                from_session.execute(table.select()).mappings().all()
            )
            logger.debug(f"Fetched {len(rows)} rows from table {table.name} for processing.")

            if not rows:
                logger.info(f"No rows to process for table: {table.name}")
                return f"No data for {table.name}"

            for chunk in chunk_iterable(rows, settings.chunk_size):
                logger.debug(f"Processing chunk of {len(chunk)} rows for table {table.name}.")
                processed_chunk = []
                for row_data in chunk:
                    # Ensure row_data is a mutable dictionary for apply_masking
                    mutable_row_data = dict(row_data)
                    if settings.masking and settings.masking.rules and table.name in settings.masking.rules:
                        # Log first 2 items by converting row_data (immutable mapping) to dict then slicing items
                        log_preview = dict(list(mutable_row_data.items())[:2])
                        logger.debug(f"Applying masking for table {table.name}, row preview: {log_preview}...")
                        processed_chunk.append(
                            apply_masking(
                                mutable_row_data, # Use the mutable dict
                                table.name,
                                settings.masking.rules,
                            )
                        )
                    else:
                        processed_chunk.append(mutable_row_data) # Use the mutable dict
                if processed_chunk:
                    to_session.execute(table.insert(), processed_chunk)
            to_session.commit()
            logger.info(f"Successfully processed and committed data for table: {table.name}")
            return f"Success: {table.name}"
    except Exception as exc:
        logger.exception(f"Error processing table {table.name}. Data for this table may not be migrated.")
        # Re-raising will make future.result() throw this exception
        raise
    # Sessions are automatically closed by the 'with' statement if they are context managers.
    # SQLAlchemy sessions obtained from sessionmaker() are context managers.

def run_alembic_cmd(
    alembic_dir: str, db_url: str, cmd: str, revision: str = ""
) -> None:
    """alembic CLI 명령 실행 (downgrade/upgrade 등)"""
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
    """to DB를 from DB revision에 맞게 다운/업그레이드"""
    logger.info(f"Starting schema sync for target DB: {to_db.engine.url.database}")
    from_rev = get_alembic_version(from_db)
    if from_rev is None:
        raise ValueError("from_db에서 alembic revision을 찾을 수 없습니다.")

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
    """메인 데이터 마이그레이션 워크플로우"""
    logger.info("Starting dump and load process.")
    # SSH 터널링 (필요시)
    if settings.source_ssh_tunnel:
        logger.info("SSH tunnel configured for source DB.")
        from_ctx = create_ssh_tunnel(settings.source_ssh_tunnel, settings.source_db)
    else:
        from_ctx = None

    if settings.target_ssh_tunnel:
        logger.info("SSH tunnel configured for target DB.")
        to_ctx = create_ssh_tunnel(settings.target_ssh_tunnel, settings.target_db)
    else:
        to_ctx = None

    with (
        from_ctx.tunnel() if from_ctx else nullcontext() as active_from_tunnel,
        to_ctx.tunnel() if to_ctx else nullcontext() as active_to_tunnel,
    ):
        if from_ctx:
            logger.info(f"SSH tunnel for source DB active: {active_from_tunnel.local_bind_address if active_from_tunnel else 'N/A'}")
        if to_ctx:
            logger.info(f"SSH tunnel for target DB active: {active_to_tunnel.local_bind_address if active_to_tunnel else 'N/A'}")

        source_db_config = get_db_config_for_connection(
            settings.source_db,
            active_from_tunnel
            if isinstance(active_from_tunnel, SSHTunnelManager)
            else None,
        )
        logger.info(f"Source DB manager created for database: {source_db_config.database}")
        target_db_config = get_db_config_for_connection(
            settings.target_db,
            active_to_tunnel
            if isinstance(active_to_tunnel, SSHTunnelManager)
            else None,
        )
        logger.info(f"Target DB manager created for database: {target_db_config.database}")

        with (
            create_db_manager(source_db_config) as from_db,
            create_db_manager(target_db_config) as to_db,
        ):
            # 스키마 동기화
            logger.info(f"Initiating schema sync between source and target on alembic_dir: {alembic_dir}.")
            sync_schema(from_db.engine, to_db.engine, alembic_dir)
            logger.info("Schema synchronization complete.")

            # Removed main from_session and to_session initialization

            try:
                # 테이블 순서 결정 using parallel execution groups
                all_metadata_tables = from_db.get_metadata() # Get metadata once
                execution_groups = get_parallel_execution_groups(all_metadata_tables)
                logger.info(f"Table execution groups for parallel processing: {[[t.name for t in group] for group in execution_groups]}")

                tables_to_exclude_names = set(settings.tables_to_exclude or [])
                tables_to_exclude_names.add("alembic_version") # Always exclude alembic_version
                logger.info(f"Final list of tables to exclude from data migration: {tables_to_exclude_names}")
                
                # Filter tables within each group
                processed_execution_groups = []
                for group in execution_groups:
                    filtered_group = [t for t in group if t.name not in tables_to_exclude_names]
                    if settings.tables_to_include:
                        tables_to_include_names = set(settings.tables_to_include)
                        filtered_group = [t for t in filtered_group if t.name in tables_to_include_names]
                    if filtered_group:
                        processed_execution_groups.append(filtered_group)
                
                logger.info(f"Starting data migration for {sum(len(g) for g in processed_execution_groups)} tables in {len(processed_execution_groups)} groups. Tables to include: {settings.tables_to_include}, Initial tables to exclude from settings: {settings.tables_to_exclude}.")

                overall_success = True
                for group_idx, group_tables in enumerate(processed_execution_groups):
                    logger.info(f"Processing table group {group_idx + 1}/{len(processed_execution_groups)} with {len(group_tables)} table(s): {[t.name for t in group_tables]}.")
                    
                    # Use settings.max_parallel_workers or fallback to os.cpu_count()
                    max_workers_setting = getattr(settings, 'max_parallel_workers', None) # Check if attribute exists
                    max_workers = max_workers_setting if max_workers_setting and max_workers_setting > 0 else os.cpu_count()
                    logger.info(f"Using up to {max_workers} workers for this group.")

                    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                        future_to_table = {executor.submit(_process_table_data, table, from_db, to_db, settings, alembic_dir): table for table in group_tables}
                        
                        for future in concurrent.futures.as_completed(future_to_table):
                            table_obj = future_to_table[future]
                            try:
                                result = future.result() # This will raise an exception if the task raised one
                                logger.info(f"Table {table_obj.name} processing completed: {result}")
                            except Exception as exc_inner:
                                logger.error(f"Table {table_obj.name} processing failed: {exc_inner}")
                                overall_success = False # Mark that at least one table failed
                                # Depending on desired strictness, one might want to raise here or break loops
                
                if not overall_success:
                    logger.warning("One or more tables failed during the parallel data migration process. Check logs for details.")
                    # Consider raising an error here if partial success is not acceptable
                    # raise RuntimeError("Parallel data migration failed for one or more tables.")
                else:
                    logger.info("All table groups processed successfully.")

            except Exception as exc:
                logger.exception("Error during dump and load process.") # Removed session rollback
                raise exc
            # Removed finally block that closes from_session and to_session
    logger.info("Dump and load process completed.")


try:
    from contextlib import nullcontext  # type: ignore
except ImportError:

    class nullcontext:
        def __enter__(self) -> Self:
            return self

        def __exit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[types.TracebackType],
        ) -> None:
            pass
