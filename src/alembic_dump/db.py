import types
from typing import Optional
import logging

from pydantic import SecretStr
from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import DBConfig
from .secrets import SecretProvider, create_secret_provider

logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self, db_config: DBConfig) -> None:
        self.config = db_config
        self._engine: Optional[Engine] = None
        self._metadata: Optional[MetaData] = None
        self._session_maker: Optional[sessionmaker] = None
        self._secret_provider = None
        logger.debug(f"DBManager initialized for driver: {db_config.driver} (database: {db_config.database})")

    def _get_secret_provider(self) -> Optional[SecretProvider]:
        """Lazy initialization of secret provider"""
        if self._secret_provider is None and self.config.secret_provider_config:
            logger.debug("Initializing secret provider.")
            self._secret_provider = create_secret_provider(
                self.config.secret_provider_config
            )
        return self._secret_provider

    def _get_config_value(self, field_name: str) -> Optional[str]:
        """Get configuration value, either directly or from secret provider"""
        logger.debug(f"Fetching DB config value for '{field_name}'.")
        # First check if value is directly provided
        value = getattr(self.config, field_name)
        if value is not None:
            logger.debug(f"Value for '{field_name}' found directly.")
            if isinstance(value, SecretStr):
                return value.get_secret_value()
            return value

        # If not directly provided and we have a secret provider, try to get from secret
        secret_provider = self._get_secret_provider()
        if secret_provider and self.config.secret_key_mapping:
            secret_key = self.config.secret_key_mapping.get(field_name)
            if secret_key:
                logger.debug(f"Fetching '{field_name}' from secret provider using key '{secret_key}'.")
                return secret_provider.get_secret_value(secret_key)
        return None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            # Get all required values, either directly or from secret provider
            username = self._get_config_value("username")
            password = self._get_config_value("password") # Keep logging of password out
            host = self._get_config_value("host")
            port = self._get_config_value("port")
            database = self._get_config_value("database")

            # Validate required fields
            if not all([username, host, database]): # Password can be optional for some drivers
                missing = []
                if not username:
                    missing.append("username")
                if not host:
                    missing.append("host")
                if not database:
                    missing.append("database")
                logger.error(f"Missing required database configuration for engine creation: {', '.join(missing)}")
                raise ValueError(
                    f"Missing required database configuration: {', '.join(missing)}"
                )
            
            logger.info(f"Creating SQLAlchemy engine for {self.config.driver} accessing database '{database}' on host '{host}'.")
            # Construct database URL
            url = f"{self.config.driver}://{username}:{password or ''}@{host}:{port or 5432}/{database}"
            self._engine = create_engine(url, **self.config.options)
        return self._engine

    def get_metadata(self) -> MetaData:
        if self._metadata is None:
            logger.info(f"Reflecting database metadata for {self.engine.url.database}.")
            self._metadata = MetaData()
            self._metadata.reflect(bind=self.engine)
        return self._metadata

    def get_tables_in_order(self) -> list[Table]:
        try:
            return self.get_metadata().sorted_tables
        except Exception as exc:
            logger.exception(f"Failed to get sorted tables: {exc}")
            raise RuntimeError(f"failed to get sorted table: {exc}") from exc

    def get_session(self) -> Session:
        if self._session_maker is None:
            logger.debug(f"Creating new SQLAlchemy session maker for {self.engine.url.database}.")
            self._session_maker = sessionmaker(bind=self.engine)
        return self._session_maker()

    def get_table_dependencies(self) -> list[tuple[str, list[str]]]:
        inspector = inspect(self.engine)
        dependencies: list[tuple[str, list[str]]] = []

        for table_name in inspector.get_table_names():
            foreign_keys = inspector.get_foreign_keys(table_name)
            referenced_tables = [fk["referred_table"] for fk in foreign_keys]
            dependencies.append((table_name, referenced_tables))
        return dependencies

    def close(self) -> None:
        if self._engine is not None:
            logger.info(f"Closing DBManager: Disposing SQLAlchemy engine for {self.engine.url.database}.")
            self._engine.dispose()
            self._engine = None
            self._metadata = None
            self._session_maker = None

    def __enter__(self) -> "DBManager":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        self.close()


def create_db_manager(db_config: DBConfig) -> DBManager:
    """DB 매니저 생성 헬퍼 함수"""
    logger.debug(f"Creating DBManager instance for DBConfig driver: {db_config.driver}, database: {db_config.database}.")
    return DBManager(db_config)
