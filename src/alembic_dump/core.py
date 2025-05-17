import subprocess
import types
from typing import Optional

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
    subprocess.run(args, check=True)


def sync_schema(from_db: Engine, to_db: Engine, alembic_dir: str) -> None:
    """to DB를 from DB revision에 맞게 다운/업그레이드"""
    from_rev = get_alembic_version(from_db)
    if from_rev is None:
        raise ValueError("from_db에서 alembic revision을 찾을 수 없습니다.")

    to_db_url = to_db.url.render_as_string(hide_password=False)
    target_current_rev = get_alembic_version(to_db)

    if target_current_rev is None:
        run_alembic_cmd(alembic_dir, to_db_url, "upgrade", from_rev)
        return
    run_alembic_cmd(alembic_dir, to_db_url, "downgrade", "base")
    run_alembic_cmd(alembic_dir, to_db_url, "upgrade", from_rev)


def dump_and_load(settings: AppSettings, alembic_dir: str) -> None:
    """메인 데이터 마이그레이션 워크플로우"""
    # SSH 터널링 (필요시)
    from_ctx = (
        create_ssh_tunnel(settings.source_ssh_tunnel, settings.source_db)
        if settings.source_ssh_tunnel
        else None
    )
    to_ctx = (
        create_ssh_tunnel(settings.target_ssh_tunnel, settings.target_db)
        if settings.target_ssh_tunnel
        else None
    )

    with (
        from_ctx.tunnel() if from_ctx else nullcontext() as active_from_tunnel,
        to_ctx.tunnel() if to_ctx else nullcontext() as active_to_tunnel,
    ):
        source_db_config = get_db_config_for_connection(
            settings.source_db,
            active_from_tunnel
            if isinstance(active_from_tunnel, SSHTunnelManager)
            else None,
        )
        target_db_config = get_db_config_for_connection(
            settings.target_db,
            active_to_tunnel
            if isinstance(active_to_tunnel, SSHTunnelManager)
            else None,
        )
        with (
            create_db_manager(source_db_config) as from_db,
            create_db_manager(target_db_config) as to_db,
        ):
            # 스키마 동기화
            sync_schema(from_db.engine, to_db.engine, alembic_dir)

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

                # 데이터 마이그레이션
                for table in tables:
                    rows = list(
                        from_db.get_session().execute(table.select()).mappings().all()
                    )

                    for chunk in chunk_iterable(rows, settings.chunk_size):
                        processed_chunk = []
                        for row_data in chunk:
                            if settings.masking and settings.masking.rules:
                                processed_chunk.append(
                                    apply_masking(
                                        dict(row_data),
                                        table.name,
                                        settings.masking.rules,
                                    )
                                )
                            else:
                                processed_chunk.append(dict(row_data))
                        if processed_chunk:
                            to_session.execute(table.insert(), processed_chunk)
                to_session.commit()
            except Exception as exc:
                to_session.rollback()
                raise exc
            finally:
                from_session.close()
                to_session.close()


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
