import subprocess
import types
from typing import Optional
import logging

from sqlalchemy import Engine
from typing_extensions import Self

from .config import AppSettings
from .db import create_db_manager
from .ssh import SSHTunnelManager, create_ssh_tunnel
from .utils import (
    apply_masking,
    chunk_iterable,
    get_alembic_version,
    get_db_config_for_connection,
    get_sorted_tables,
)

logger = logging.getLogger(__name__)


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

            from_session = from_db.get_session()
            to_session = to_db.get_session()

            try:
                # 테이블 순서 결정
                tables = get_sorted_tables(from_db.get_metadata())

                tables_to_exclude_names = set(settings.tables_to_exclude or [])
                tables = [t for t in tables if t.name not in tables_to_exclude_names]

                if settings.tables_to_include:
                    tables_to_include_names = set(settings.tables_to_include)
                    tables = [t for t in tables if t.name in tables_to_include_names]
                
                logger.info(f"Starting data migration for {len(tables)} tables. Tables to include: {settings.tables_to_include}, Tables to exclude: {settings.tables_to_exclude}.")

                # 데이터 마이그레이션
                for table in tables:
                    logger.info(f"Processing table: {table.name}")
                    rows = list(
                        from_db.get_session().execute(table.select()).mappings().all()
                    )
                    logger.debug(f"Fetched {len(rows)} rows from table {table.name}.")

                    for chunk in chunk_iterable(rows, settings.chunk_size):
                        logger.debug(f"Processing chunk of {len(chunk)} rows for table {table.name}.")
                        processed_chunk = []
                        for row_data in chunk:
                            if settings.masking and settings.masking.rules and table.name in settings.masking.rules:
                                logger.debug(f"Applying masking for table {table.name}")
                                processed_chunk.append(
                                    apply_masking(
                                        dict(row_data),
                                        table.name,
                                        settings.masking.rules,
                                    )
                                )
                            else:
                                logger.debug(f"No masking configured for table {table.name}")
                                processed_chunk.append(dict(row_data))
                        if processed_chunk:
                            to_session.execute(table.insert(), processed_chunk)
                to_session.commit()
                logger.info("Data migration committed successfully to target database.")
            except Exception as exc:
                logger.exception("Error during dump and load process. Rolling back session.")
                to_session.rollback()
                raise exc
            finally:
                from_session.close()
                to_session.close()
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
