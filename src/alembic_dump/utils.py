import logging
from collections.abc import Generator, Iterable
from typing import Any, Optional

from sqlalchemy import MetaData, Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from alembic_dump.config import DBConfig
from alembic_dump.ssh import SSHTunnelManager

logger = logging.getLogger(__name__)


def get_sorted_tables(metadata: MetaData) -> list[Table]:
    """
    Returns topologically sorted tables from SQLAlchemy MetaData considering foreign key dependencies.
    If circular dependencies exist, logs a warning and returns tables without specific ordering.
    """
    try:
        sorted_tables = list(metadata.sorted_tables)
        return sorted_tables
    except Exception as e:
        logger.warning(f"Error during table topological sort: {e}")
        # Fallback: return without specific ordering
        return list(metadata.tables.values())


def detect_circular_dependencies(metadata: MetaData) -> list[set[str]]:
    """
    Detects groups of tables with circular foreign key references in MetaData.
    Returns: List of sets, where each set contains table names forming a cycle.
    """
    from collections import defaultdict

    graph = defaultdict(set)
    for table in metadata.tables.values():
        for fk in table.foreign_keys:
            graph[table.name].add(fk.column.table.name)

    visited = set()
    stack = []
    cycles = []

    def visit(node, path):
        if node in path:
            cycle = set(path[path.index(node) :])
            if cycle not in cycles:
                cycles.append(cycle)
            return
        if node in visited:
            return
        visited.add(node)
        path.append(node)
        for neighbor in graph[node]:
            visit(neighbor, path)
        path.pop()

    for node in list(graph.keys()):
        visit(node, [])

    return cycles


def chunk_iterable(
    iterable: Iterable, chunk_size: int
) -> Generator[list[Any], None, None]:
    """
    Splits an iterable into chunks of specified size.
    Useful for batch processing of large datasets.

    Args:
        iterable: The input iterable to be chunked
        chunk_size: Maximum size of each chunk

    Yields:
        List containing items from the iterable, with length <= chunk_size
    """
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def get_alembic_version(engine: Engine) -> Optional[str]:
    """
    Queries the current Alembic revision from the database.

    Args:
        engine: SQLAlchemy engine connected to the database

    Returns:
        Current revision string or None if not found/error occurs
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            row = result.fetchone()
            if row:
                return row[0]
    except SQLAlchemyError as e:
        logger.warning(f"Failed to retrieve Alembic version: {e}")
    except Exception as exc:
        logger.warning(f"Failed to retrieve Alembic version: {exc}")
    return None


def filter_tables(
    all_tables: list[Table],
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
) -> list[Table]:
    """
    Filters table list based on inclusion/exclusion rules.

    Args:
        all_tables: List of all available tables
        include: List of table names to include (if None, includes all)
        exclude: List of table names to exclude

    Returns:
        Filtered list of tables
    """
    tables = all_tables
    if include:
        tables = [t for t in tables if t.name in include]
    if exclude:
        tables = [t for t in tables if t.name not in exclude]
    return tables


def mask_value(value: Any, rule: dict[str, Any]) -> Any:
    """
    Masks a single value based on the provided masking rule.
    This is a simplified version; for extended implementation, refer to masking.py.

    Args:
        value: The value to be masked
        rule: Dictionary containing masking strategy and parameters

    Returns:
        Masked value
    """
    strategy = rule.get("strategy")
    if strategy == "null":
        return None
    elif strategy == "hash":
        import hashlib

        salt = rule.get("hash_salt", "")
        return hashlib.sha256((str(value) + str(salt)).encode()).hexdigest()
    elif strategy == "partial":
        keep = rule.get("partial_keep_chars", 4)
        s = str(value)
        return "*" * max(0, len(s) - keep) + s[-keep:]
    elif strategy == "faker":
        from faker import Faker

        provider = rule.get("faker_provider", "name")
        fake = Faker()
        return getattr(fake, provider)()
    # Custom strategies like encryption need separate implementation
    return value


def apply_masking(
    row: dict[str, Any], table: str, masking_rules: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """
    Applies masking rules to a single row (dictionary).

    Args:
        row: Dictionary representing a single database row
        table: Name of the table the row belongs to
        masking_rules: Dictionary of masking rules in the format {table: {column: rule}}

    Returns:
        Dictionary with masked values
    """
    rules = masking_rules.get(table, {})
    masked = {}
    for col, val in row.items():
        rule = rules.get(col)
        if rule:
            masked[col] = mask_value(val, rule)
        else:
            masked[col] = val
    return masked


def get_db_config_for_connection(
    original_db_config: DBConfig,
    active_ssh_tunnel: Optional[SSHTunnelManager],
    db_name_for_log: str = "Database",
) -> DBConfig:
    db_config_to_use = original_db_config.model_copy(deep=True)
    if active_ssh_tunnel is None:
        return db_config_to_use
    db_config_to_use.port = active_ssh_tunnel.local_bind_address[1]
    db_config_to_use.host = active_ssh_tunnel.local_bind_address[0]
    return db_config_to_use


def get_parallel_execution_groups(metadata: MetaData) -> list[list[Table]]:
    """
    Determines groups of tables that can be processed in parallel based on FK dependencies.

    Args:
        metadata: SQLAlchemy MetaData object.

    Returns:
        List of lists of Table objects, where each inner list is a group of tables
        that can be processed in parallel.
    """
    all_tables = list(metadata.tables.values())
    if not all_tables:
        return []

    table_map = {table.name: table for table in all_tables}
    prerequisites: dict[str, set[str]] = {
        table.name: {fk.column.table.name for fk in table.foreign_keys}
        for table in all_tables
    }

    table_levels: dict[str, int] = {}  # Stores the calculated level for each table name
    execution_groups: list[list[Table]] = []

    while len(table_levels) < len(all_tables):
        current_level_table_names: list[str] = []
        for table in all_tables:
            if table.name not in table_levels:
                # Check if all prerequisites for this table are already in table_levels
                if all(
                    prereq_name in table_levels
                    for prereq_name in prerequisites[table.name]
                ):
                    current_level_table_names.append(table.name)

        if not current_level_table_names:
            # If no tables can be added to the current level, but not all tables are processed,
            # it indicates a cycle or an unhandled dependency.
            if len(table_levels) < len(all_tables):
                remaining_tables = [
                    t.name for t in all_tables if t.name not in table_levels
                ]
                logger.warning(
                    "Could not determine execution level for all tables. "
                    "Potential cycle or unhandled dependency. Remaining tables: %s",
                    remaining_tables,
                )
                # Add remaining tables as a single group to ensure they are processed,
                # though not in parallel as originally intended for this batch.
                # The caller can decide how to handle this (e.g., sequential processing).
                # For now, we group them and let the process continue.
                # This part could be adapted based on desired error handling (e.g., raise an exception).
                if remaining_tables:
                    current_level_idx = len(execution_groups)
                    for name in remaining_tables:
                        table_levels[name] = current_level_idx
                    current_group_tables = [table_map[name] for name in remaining_tables]
                    execution_groups.append(current_group_tables)
                break  # Exit the main loop
            else:
                # All tables processed
                break

        current_level_idx = len(execution_groups)
        for name in current_level_table_names:
            table_levels[name] = current_level_idx

        current_group_tables = [
            table_map[name] for name in current_level_table_names
        ]
        execution_groups.append(current_group_tables)

    logger.debug(
        f"Determined parallel execution groups: {[[t.name for t in group] for group in execution_groups]}"
    )
    return execution_groups
