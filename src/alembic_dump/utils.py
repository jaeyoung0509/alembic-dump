import logging
from collections.abc import Generator, Iterable
from typing import Any, Optional

from sqlalchemy import MetaData, Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from alembic_dump.config import DBConfig
from alembic_dump.ssh import SSHTunnelManager

logger = logging.getLogger(__name__)


from collections import deque

def get_sorted_tables(metadata: MetaData) -> list[list[Table]]:
    """
    Returns a list of table groups, where each group consists of tables that can be processed in parallel.
    The groups are ordered by dependency level. Tables in earlier groups are typically prerequisites
    for tables in later groups. Within each group, tables are sorted alphabetically by name.

    This implementation uses Kahn's algorithm for topological sorting to determine levels.

    Args:
        metadata: SQLAlchemy MetaData object containing all table definitions.

    Returns:
        A list of lists of Table objects (e.g., [[TableA, TableE], [TableB, TableC], [TableD]]).
        Returns an empty list if metadata contains no tables.
        If an error occurs, it logs a warning and may return a single group containing all tables
        sorted by SQLAlchemy's default dependency sort, or an empty list if that also fails.
    """
    table_objects = list(metadata.tables.values())
    if not table_objects:
        return []

    try:
        adj = {table.name: [] for table in table_objects}
        in_degree = {table.name: 0 for table in table_objects}
        table_map = {table.name: table for table in table_objects}

        # Build adjacency list and in-degree map
        for table in table_objects:
            for fk in table.foreign_keys:
                # A foreign key means 'table' depends on 'fk.column.table'
                referenced_table_name = fk.column.table.name
                dependent_table_name = table.name

                # Ensure the referenced table is part of the current metadata
                if referenced_table_name in table_map:
                    # Check if it's a self-referential FK for the purpose of graph building.
                    # For Kahn's, a self-reference doesn't increase in-degree from *another* table.
                    if referenced_table_name != dependent_table_name:
                        adj[referenced_table_name].append(dependent_table_name)
                        in_degree[dependent_table_name] += 1
                else:
                    # This case might occur if FK points to a table outside the current MetaData scope
                    # (e.g. different schema not being reflected). For this algorithm, we only
                    # consider dependencies between tables present in the input `metadata`.
                    logger.debug(
                        f"Table '{dependent_table_name}' has a foreign key to '{referenced_table_name}', "
                        "which is not found in the current metadata's table map. This dependency will be ignored for sorting."
                    )


        # Initialize queue with tables that have no dependencies (in-degree 0)
        queue = deque()
        for table_name in in_degree:
            if in_degree[table_name] == 0:
                queue.append(table_name)
        
        result_groups_of_names = []
        processed_table_count = 0

        while queue:
            current_level_size = len(queue)
            current_group_names = []
            
            for _ in range(current_level_size):
                table_name = queue.popleft()
                current_group_names.append(table_name)
                processed_table_count +=1
                
                # For each successor, decrement its in-degree
                for successor_name in adj[table_name]:
                    in_degree[successor_name] -= 1
                    if in_degree[successor_name] == 0:
                        queue.append(successor_name)
            
            # Sort tables within the current group alphabetically by name
            current_group_names.sort()
            result_groups_of_names.append(current_group_names)

        if processed_table_count != len(table_objects):
            # This indicates a cycle or an issue with dependency resolution not caught otherwise.
            # SQLAlchemy's metadata.sorted_tables often handles cycles by breaking them.
            # If this custom implementation fails, it's a sign of complex structure or error.
            logger.warning(
                f"Could not sort all tables by dependency level ({processed_table_count} out of {len(table_objects)} processed). "
                "This might indicate circular dependencies not resolved by simple FK counting, or an issue in graph construction. "
                "Falling back to a single group based on metadata.sorted_tables."
            )
            # Fallback: return a single group, with tables sorted by SQLAlchemy's default.
            # This at least provides a valid (though not optimally grouped) list.
            try:
                return [list(metadata.sorted_tables)]
            except Exception as fallback_e:
                logger.error(f"Fallback to metadata.sorted_tables also failed: {fallback_e}")
                return [table_objects] # Raw list as last resort, in one group

        # Convert names back to Table objects
        result_groups_of_tables = []
        for group_of_names in result_groups_of_names:
            result_groups_of_tables.append([table_map[name] for name in group_of_names])
            
        return result_groups_of_tables

    except Exception as e:
        logger.exception(
            f"An unexpected error occurred during table dependency grouping: {e}. "
            "Falling back to a single group based on metadata.sorted_tables if possible."
        )
        try:
            # Attempt to return a single group using SQLAlchemy's default sort order
            return [list(metadata.sorted_tables)]
        except Exception as fallback_e:
            logger.error(f"Fallback to metadata.sorted_tables also failed during exception handling: {fallback_e}")
            # As a last resort, return all tables in a single group, unsorted by this function.
            return [table_objects] if table_objects else []


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
    db_name_for_log: str = "Database",  # Intended for logging context, currently not used in function body
) -> DBConfig:
    """
    Adjusts a copy of the DBConfig to use SSH tunnel parameters if an active tunnel is provided.
    Otherwise, returns a copy of the original DBConfig.

    Args:
        original_db_config: The base database configuration.
        active_ssh_tunnel: An active SSHTunnelManager instance, or None if no tunnel is used.
        db_name_for_log: A descriptive name for the database (e.g., "Source DB", "Target DB")
                         intended for use in log messages by the caller. Currently not used within this function itself.
    
    Returns:
        A new DBConfig instance, potentially modified to use SSH tunnel host and port.
    """
    db_config_to_use = original_db_config.model_copy(deep=True)
    if active_ssh_tunnel is None:
        return db_config_to_use
    db_config_to_use.port = active_ssh_tunnel.local_bind_address[1]
    db_config_to_use.host = active_ssh_tunnel.local_bind_address[0]
    return db_config_to_use
