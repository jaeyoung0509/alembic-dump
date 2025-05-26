import pytest
from unittest import mock
from unittest.mock import MagicMock, call, ANY
from sqlalchemy import MetaData, Table as SATable # Column, Integer, ForeignKey removed
import concurrent.futures
from typing import Any, Dict, List, Generator, Callable, Iterable # Added typing imports

from alembic_dump.config import AppSettings, DBConfig
from alembic_dump.core import dump_and_load
from alembic_dump.utils import chunk_iterable, detect_circular_dependencies

# --- Pytest Fixture for Core Mocks ---
@pytest.fixture
def core_mocks_fx() -> Generator[Dict[str, Any], None, None]:
    """Provides common mocks for testing dump_and_load using unittest.mock."""
    patchers: List[mock.patch] = [] # Type hint for patchers list
    
    p_create_db_manager = mock.patch("alembic_dump.core.create_db_manager")
    mock_create_db_manager = p_create_db_manager.start()
    patchers.append(p_create_db_manager)

    p_sync_schema = mock.patch("alembic_dump.core.sync_schema")
    mock_sync_schema = p_sync_schema.start()
    patchers.append(p_sync_schema)

    p_get_sorted_tables = mock.patch("alembic_dump.core.get_sorted_tables")
    mock_get_sorted_tables = p_get_sorted_tables.start()
    patchers.append(p_get_sorted_tables)

    p_process_table_data = mock.patch("alembic_dump.core._process_table_data")
    mock_process_table_data = p_process_table_data.start()
    patchers.append(p_process_table_data)

    p_thread_pool_executor_cls = mock.patch("concurrent.futures.ThreadPoolExecutor")
    mock_thread_pool_executor_cls = p_thread_pool_executor_cls.start()
    patchers.append(p_thread_pool_executor_cls)
    
    p_as_completed = mock.patch("concurrent.futures.as_completed")
    mock_as_completed_func = p_as_completed.start()
    patchers.append(p_as_completed)

    mock_from_db_manager = MagicMock()
    mock_to_db_manager = MagicMock()
    mock_create_db_manager.side_effect = [mock_from_db_manager, mock_to_db_manager]
    
    mock_metadata = MagicMock(spec=MetaData)
    mock_from_db_manager.get_metadata.return_value = mock_metadata
    
    mock_executor_instance = MagicMock(spec=concurrent.futures.ThreadPoolExecutor)
    mock_thread_pool_executor_cls.return_value.__enter__.return_value = mock_executor_instance
    
    submitted_tasks_info: List[Dict[str, Any]] = [] 
    
    def mock_submit_fn(
        func: Callable[..., Any], 
        *args: Any, 
        **kwargs: Any
    ) -> MagicMock:
        future = MagicMock(spec=concurrent.futures.Future)
        # args[0] is expected to be a table-like object with a 'name' attribute
        future.result = MagicMock(return_value=args[0].name) 
        future.exception_value = None
        submitted_tasks_info.append({
            'function': func, 'args': args, 'kwargs': kwargs, 
            'future': future, 'table_name': args[0].name
        })
        return future

    mock_executor_instance.submit.side_effect = mock_submit_fn
    mock_executor_instance.submitted_tasks_info = submitted_tasks_info

    def actual_mock_as_completed_side_effect(
        fs: Iterable[concurrent.futures.Future]
    ) -> List[concurrent.futures.Future]:
        ordered_futures_from_submitted = [
            task['future'] for task in submitted_tasks_info if task['future'] in fs
        ]
        return ordered_futures_from_submitted
    mock_as_completed_func.side_effect = actual_mock_as_completed_side_effect

    mocks_dict: Dict[str, Any] = {
        "create_db_manager": mock_create_db_manager,
        "sync_schema": mock_sync_schema,
        "get_sorted_tables": mock_get_sorted_tables,
        "process_table_data": mock_process_table_data,
        "thread_pool_executor_cls": mock_thread_pool_executor_cls,
        "executor_instance": mock_executor_instance,
        "from_db_manager": mock_from_db_manager,
        "to_db_manager": mock_to_db_manager,
        "metadata": mock_metadata,
        "submitted_tasks_info": submitted_tasks_info,
        "as_completed": mock_as_completed_func,
    }

    yield mocks_dict

    for patcher in patchers:
        patcher.stop()

# --- Mock Table Factory ---
def create_mock_table(name: str) -> MagicMock:
    """Creates a MagicMock for a SQLAlchemy Table with a name."""
    table = MagicMock(spec=SATable)
    table.name = name
    return table

# --- Tests for dump_and_load ---

def test_dump_and_load_processes_groups_sequentially(
    core_mocks_fx: Dict[str, Any]
) -> None:
    """
    Tests that table groups are processed sequentially, and tables within a group
    are processed before moving to the next group.
    """
    table_a = create_mock_table("table_a")
    table_b = create_mock_table("table_b")
    table_c = create_mock_table("table_c")
    table_d = create_mock_table("table_d")

    core_mocks_fx["get_sorted_tables"].return_value = [
        [table_a, table_b], [table_c, table_d]
    ]
    
    processed_order: List[str] = []
    def sequential_process_table_data(
        table: Any, *args: Any, **kwargs: Any
    ) -> str:
        processed_order.append(table.name)
        return table.name 
    core_mocks_fx["process_table_data"].side_effect = sequential_process_table_data
    
    settings = AppSettings(
        source_db=DBConfig(driver="postgresql", database="src"),
        target_db=DBConfig(driver="postgresql", database="tgt"),
    )
    
    dump_and_load(settings, "/fake/alembic")

    core_mocks_fx["get_sorted_tables"].assert_called_once_with(
        core_mocks_fx["metadata"]
    )
    expected_call_order = [
        call(table_a, ANY, ANY, ANY, ANY), 
        call(table_b, ANY, ANY, ANY, ANY),
        call(table_c, ANY, ANY, ANY, ANY), 
        call(table_d, ANY, ANY, ANY, ANY),
    ]
    core_mocks_fx["process_table_data"].assert_has_calls(
        expected_call_order, any_order=False
    )
    assert processed_order == ["table_a", "table_b", "table_c", "table_d"]

    assert core_mocks_fx["thread_pool_executor_cls"].call_count == 2
    args_list = core_mocks_fx["thread_pool_executor_cls"].call_args_list
    assert args_list[0][1]["max_workers"] == 2
    assert args_list[1][1]["max_workers"] == 2


def test_dump_and_load_parallel_within_group(
    core_mocks_fx: Dict[str, Any]
) -> None:
    """
    Tests that all tables within a single dependency group are submitted to the
    ThreadPoolExecutor and that max_workers is set to the group size.
    """
    table_x = create_mock_table("table_x")
    table_y = create_mock_table("table_y")
    table_z = create_mock_table("table_z")

    core_mocks_fx["get_sorted_tables"].return_value = [[table_x, table_y, table_z]]
    
    settings = AppSettings(
        source_db=DBConfig(driver="postgresql", database="src"),
        target_db=DBConfig(driver="postgresql", database="tgt"),
    )
    
    dump_and_load(settings, "/fake/alembic")

    core_mocks_fx["thread_pool_executor_cls"].assert_called_once()
    assert core_mocks_fx["thread_pool_executor_cls"].call_args[1]["max_workers"] == 3

    executor_instance = core_mocks_fx["executor_instance"]
    assert executor_instance.submit.call_count == 3
    submitted_table_names = sorted(
        [task['table_name'] for task in core_mocks_fx["submitted_tasks_info"]]
    )
    assert submitted_table_names == ["table_x", "table_y", "table_z"]


def test_dump_and_load_error_stops_further_group_processing(
    core_mocks_fx: Dict[str, Any]
) -> None:
    """
    Tests that an error in one table stops processing of that table's group
    and any subsequent groups.
    """
    table_g1_t1 = create_mock_table("g1_t1")
    table_g1_t2 = create_mock_table("g1_t2") 
    table_g2_t1 = create_mock_table("g2_t1")

    core_mocks_fx["get_sorted_tables"].return_value = [
        [table_g1_t1, table_g1_t2], [table_g2_t1]
    ]
    
    original_submit_fn = core_mocks_fx["executor_instance"].submit.side_effect
    processed_tables_before_error: List[str] = []

    def custom_submit_for_error_test(
        func: Callable[..., Any], 
        table_obj: Any, 
        *args: Any, 
        **kwargs: Any
    ) -> MagicMock:
        processed_tables_before_error.append(table_obj.name)
        future = original_submit_fn(func, table_obj, *args, **kwargs)
        if table_obj.name == "g1_t2":
            error_to_raise = ValueError(f"Failure processing {table_obj.name}")
            future.result = MagicMock(side_effect=error_to_raise)
            future.exception_value = error_to_raise
        return future
    core_mocks_fx["executor_instance"].submit.side_effect = custom_submit_for_error_test
    
    settings = AppSettings(
        source_db=DBConfig(driver="postgresql", database="src"),
        target_db=DBConfig(driver="postgresql", database="tgt"),
    )
    
    with pytest.raises(Exception, match="Data migration failed for one or more tables."):
        dump_and_load(settings, "/fake/alembic")

    assert "g1_t1" in processed_tables_before_error
    assert "g1_t2" in processed_tables_before_error
    assert "g2_t1" not in processed_tables_before_error 
    
    assert core_mocks_fx["process_table_data"].call_count >= 1
    core_mocks_fx["thread_pool_executor_cls"].assert_called_once() 
    assert core_mocks_fx["thread_pool_executor_cls"].call_args[1]["max_workers"] == 2

# --- Existing module-level tests ---
def test_chunk_iterable() -> None:
    data = list(range(7))
    chunks = list(chunk_iterable(data, 3))
    assert chunks == [[0, 1, 2], [3, 4, 5], [6]]

def test_get_sorted_tables_and_circular_module_level_actual_util() -> None:
    from alembic_dump.utils import get_sorted_tables as actual_get_sorted_tables_util
    from sqlalchemy import Column, Integer, ForeignKey # Now imported locally

    metadata = MetaData()
    parent = SATable("parent_util", metadata, Column("id", Integer, primary_key=True))
    SATable(
        "child_util", metadata, Column("id", Integer, primary_key=True),
        Column("parent_id", Integer, ForeignKey(parent.c.id)),
    )
    table_groups = actual_get_sorted_tables_util(metadata)
    assert len(table_groups) == 2
    assert table_groups[0][0].name == "parent_util"
    assert table_groups[1][0].name == "child_util"
    
    cycles = detect_circular_dependencies(metadata)
    assert cycles == []
