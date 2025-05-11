from contextlib import contextmanager
from typing import Optional

from sshtunnel import SSHTunnelForwarder  # type: ignore

from .config import DBConfig, SSHConfig


class SSHTunnelManager:
    def __init__(self, ssh_config: SSHConfig, db_config: DBConfig):
        self.ssh_config = ssh_config
        self.db_config = db_config
        self._tunnel: Optional[SSHTunnelForwarder] = None

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

        self._tunnel = SSHTunnelForwarder(
            ssh_address_or_host=(self.ssh_config.host, self.ssh_config.port),
            ssh_username=self.ssh_config.username,
            ssh_password=self.ssh_config.password.get_secret_value()
            if self.ssh_config.password
            else None,
            ssh_pkey=self.ssh_config.private_key_path,
            remote_bind_address=(self.db_config.host, self.db_config.port or 5432),
            local_bind_address=("127.0.0.1", 0),
            allow_agent=False,
            ssh_host_key=None,
        )

        self._tunnel.start()

    def stop(self) -> None:
        if self._tunnel is not None:
            self._tunnel.stop()
            self._tunnel = None

    @contextmanager
    def tunnel(self):
        try:
            self.start()
            yield self
        finally:
            self.stop()


def create_ssh_tunnel(ssh_config: SSHConfig, db_config: DBConfig) -> SSHTunnelManager:
    return SSHTunnelManager(ssh_config, db_config)
