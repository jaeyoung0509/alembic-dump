import logging
from typing import Any

import pytest
from sqlalchemy import Engine, text

from alembic_dump.config import DBConfig, SSHConfig
from alembic_dump.core import run_alembic_cmd
from alembic_dump.db import create_db_manager
from alembic_dump.ssh import create_ssh_tunnel

logger = logging.getLogger(__name__)


def _apply_alembic_migrations(
    db_engine: Engine, alembic_dir: str, revision: str = "head"
) -> None:
    db_url_for_alembic = str(db_engine.url.render_as_string(hide_password=False))
    try:
        run_alembic_cmd(
            alembic_dir=alembic_dir,
            db_url=db_url_for_alembic,
            cmd="upgrade",
            revision=revision,
        )
    except Exception:
        logger.error(
            f"Failed to apply Alembic migrations to {db_url_for_alembic}. Revision: {revision}"
        )
        raise


@pytest.mark.integration_ssh
def test_ssh_tunnel_direct_db_connection(
    ssh_server_container: dict[str, Any],  # SSH 서버 컨테이너
    source_pg_container: dict[str, Any],  # 소스 PG 컨테이너
) -> None:
    logger.info("Starting test_ssh_tunnel direct db connection...")
    ssh_config = SSHConfig(
        host=ssh_server_container["host_for_host_machine"],
        port=ssh_server_container["port_on_host"],  # Changed from port_internal
        username=ssh_server_container["user"],
        private_key_path=ssh_server_container["private_key_path"],
    )
    logger.info(f"Attempting SSH connection to {ssh_config.host}:{ssh_config.port}")

    remote_db_config_for_tunnel = DBConfig(
        driver="postgresql",
        host=source_pg_container["host_for_docker_network"],
        port=source_pg_container["port_internal"],
        username=source_pg_container["user"],
        password=source_pg_container["password"],
        database=source_pg_container["database"],
    )
    logger.info(
        f"Target database: {remote_db_config_for_tunnel.host}:{remote_db_config_for_tunnel.port}"
    )

    ssh_tunnel_manager = create_ssh_tunnel(ssh_config, remote_db_config_for_tunnel)

    with ssh_tunnel_manager.tunnel() as tunnel:
        assert tunnel is not None
        assert tunnel.is_active is True

        local_bind_host, local_bind_port = tunnel.local_bind_address

        tunneled_db_config = DBConfig(
            driver="postgresql",
            host=local_bind_host,
            port=local_bind_port,
            username=source_pg_container["user"],
            password=source_pg_container["password"],
            database=source_pg_container["database"],
        )

        with (
            create_db_manager(tunneled_db_config) as db_manager,
            db_manager.engine.connect() as connection,
        ):
            result = connection.execute(text("SELECT 1")).scalar_one()
            assert result == 1
