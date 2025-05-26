from collections.abc import Generator
from contextlib import contextmanager
from typing import Optional
import logging

from sshtunnel import SSHTunnelForwarder  # type: ignore
from typing_extensions import Self

from .config import DBConfig, SSHConfig

logger = logging.getLogger(__name__)


class SSHTunnelManager:
    def __init__(self, ssh_config: SSHConfig, db_config: DBConfig) -> None:
        self.ssh_config = ssh_config
        self.db_config = db_config
        self._tunnel: Optional[SSHTunnelForwarder] = None
        logger.debug(f"SSHTunnelManager initialized for SSH host {ssh_config.host}:{ssh_config.port}, remote DB {db_config.host}:{db_config.port or 5432})")

    @property
    def is_active(self) -> bool:
        return self._tunnel is not None and self._tunnel.is_active

    @property
    def local_bind_address(self) -> tuple[str, int]:
        if not self.is_active:
            raise RuntimeError("SSH tunnel is not active")
        if self._tunnel is None:
            raise RuntimeError("SSH tunnel is not active")
        return self._tunnel.local_bind_address

    def start(self) -> None:
        if self.is_active:
            return
        
        logger.info(f"Starting SSH tunnel: {self.ssh_config.username}@{self.ssh_config.host}:{self.ssh_config.port} -> remote {self.db_config.host}:{self.db_config.port or 5432}.")
        self._tunnel = SSHTunnelForwarder(
            ssh_address_or_host=(self.ssh_config.host, self.ssh_config.port),
            ssh_username=self.ssh_config.username,
            ssh_password=(
                self.ssh_config.password.get_secret_value()
                if self.ssh_config.password
                else None
            ),
            ssh_pkey=self.ssh_config.private_key_path,
            remote_bind_address=(self.db_config.host, self.db_config.port or 5432),
            local_bind_address=("127.0.0.1", 0), # Bind to a random available port
            allow_agent=False,
            ssh_host_key=None, # Consider adding host key verification for security
        )

        if self._tunnel is None:
            # This case should ideally not be reached if SSHTunnelForwarder constructor behaves as expected
            logger.error("SSHTunnelForwarder object creation unexpectedly resulted in None.")
            raise ValueError("tunnel can not be None")
        try:
            self._tunnel.start()
            logger.info(f"SSH tunnel active. Local bind: {self.local_bind_address[0]}:{self.local_bind_address[1]}.")
        except Exception as e:
            logger.exception(f"Failed to start SSH tunnel: {e}")
            # Clean up tunnel object if start failed
            if self._tunnel:
                self._tunnel.stop() # Ensure any partial state is cleaned
                self._tunnel = None
            raise # Re-raise the exception to signal failure

    def stop(self) -> None:
        if self._tunnel is not None and self._tunnel.is_active:
            local_bind_info = f"{self.local_bind_address[0]}:{self.local_bind_address[1]}"
            logger.info(f"Stopping SSH tunnel to {self.ssh_config.host}. Local bind was: {local_bind_info}.")
            self._tunnel.stop()
            self._tunnel = None

    @contextmanager
    def tunnel(self) -> Generator[Self, None, None]:
        try:
            self.start()
            yield self
        finally:
            self.stop()


def create_ssh_tunnel(ssh_config: SSHConfig, db_config: DBConfig) -> SSHTunnelManager:
    logger.debug(f"Creating SSHTunnelManager for SSH {ssh_config.host}, DB {db_config.host or 'default_port_db'}.")
    return SSHTunnelManager(ssh_config, db_config)
