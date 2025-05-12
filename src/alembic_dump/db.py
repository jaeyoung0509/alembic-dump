from typing import Any, Optional

from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import DBConfig


class DBManager:
    def __init__(self, db_config: DBConfig) -> None:
        self.config = db_config
        self._engine: Optional[Engine] = None
        self._metadata: Optional[MetaData] = None
        self._session_maker: Optional[sessionmaker] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            url = f"{self.config.driver}://{self.config.username}:{self.config.password.get_secret_value()}@{self.config.host}:{self.config.port or 5432}/{self.config.database}"
            self._engine = create_engine(url, **self.config.options)
        return self._engine

    def get_metadata(self) -> MetaData:
        if self._metadata is None:
            self._metadata = MetaData()
            self._metadata.reflect(bind=self.engine)
        return self._metadata

    def get_tables_in_order(self) -> list[Table]:
        try:
            return self.get_metadata().sorted_tables
        except Exception as exc:
            raise RuntimeError(f"failed to get sorted table: {exc}") from exc

    def get_session(self) -> Session:
        if self._session_maker is None:
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
            self._engine.dispose()
            self._engine = None
            self._metadata = None
            self._session_maker = None

    def __enter__(self) -> "DBManager":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def create_db_manager(db_config: DBConfig) -> DBManager:
    """DB 매니저 생성 헬퍼 함수"""
    return DBManager(db_config)
