import pytest
from unittest.mock import MagicMock, call, ANY
from sqlalchemy import MetaData, Table as SATable # Renamed to avoid clash
import concurrent.futures # For spec=concurrent.futures.Future

from alembic_dump.config import AppSettings, DBConfig
from alembic_dump.core import dump_and_load, _process_table_data # For spec
from alembic_dump.utils import chunk_iterable, detect_circular_dependencies # Kept for existing tests

# --- Pytest Fixture for Core Mocks ---
@pytest.fixture
def core_mocks_fx(mocker):
    """Provides common mocks for testing dump_and_load."""
    mock_create_db_manager = mocker.patch("alembic_dump.core.create_db_manager")
    mock_sync_schema = mocker.patch("alembic_dump.core.sync_schema")
    mock_get_sorted_tables = mocker.patch("alembic_dump.core.get_sorted_tables")
    mock_process_table_data = mocker.patch("alembic_dump.core._process_table_data")
    mock_thread_pool_executor_cls = mocker.patch("concurrent.futures.ThreadPoolExecutor")

    # Configure common mock returns
    mock_from_db_manager = MagicMock()
    mock_to_db_manager = MagicMock()
    mock_create_db_manager.side_effect = [mock_from_db_manager, mock_to_db_manager]
    
    mock_metadata = MagicMock(spec=MetaData)
    mock_from_db_manager.get_metadata.return_value = mock_metadata
    
    mock_executor_instance = MagicMock(spec=concurrent.futures.ThreadPoolExecutor)
    mock_thread_pool_executor_cls.return_value.__enter__.return_value = mock_executor_instance
    
    # To store submitted tasks for assertion
    submitted_tasks_info = [] 
    
    def mock_submit_fn(func, *args, **kwargs):
        # func will be _process_table_data
        # args[0] will be the table object
        future = MagicMock(spec=concurrent.futures.Future)
        # Simulate task success by default, result is table name
        future.result = MagicMock(return_value=args[0].name) 
        future.exception_value = None # For error simulation
        
        submitted_tasks_info.append({
            'function': func, 
            'args': args, 
            'kwargs': kwargs, 
            'future': future,
            'table_name': args[0].name
        })
        return future

    mock_executor_instance.submit.side_effect = mock_submit_fn

    # Allow tests to access submitted_tasks_info to control future results or assert calls
    mock_executor_instance.submitted_tasks_info = submitted_tasks_info


    # Helper to simulate completion of futures in order of submission for deterministic tests
    def mock_as_completed(fs):
        # fs is a list of future objects passed from the code
        # We need to find these in our submitted_tasks_info and yield them
        ordered_futures_from_submitted = [task['future'] for task in submitted_tasks_info if task['future'] in fs]
        return ordered_futures_from_submitted

    mocker.patch("concurrent.futures.as_completed", side_effect=mock_as_completed)

    return {
        "create_db_manager": mock_create_db_manager,
        "sync_schema": mock_sync_schema,
        "get_sorted_tables": mock_get_sorted_tables,
        "process_table_data": mock_process_table_data,
        "thread_pool_executor_cls": mock_thread_pool_executor_cls,
        "executor_instance": mock_executor_instance,
        "from_db_manager": mock_from_db_manager,
        "to_db_manager": mock_to_db_manager,
        "metadata": mock_metadata,
        "submitted_tasks_info": submitted_tasks_info, # Direct access for tests
    }

# --- Mock Table Factory ---
def create_mock_table(name: str) -> MagicMock:
    """Creates a MagicMock for a SQLAlchemy Table with a name."""
    table = MagicMock(spec=SATable)
    table.name = name
    return table

# --- Tests for dump_and_load ---

def test_dump_and_load_processes_groups_sequentially(core_mocks_fx, mocker):
    """
    Tests that table groups are processed sequentially, and tables within a group are processed
    before moving to the next group.
    """
    # Mock tables
    table_a = create_mock_table("table_a")
    table_b = create_mock_table("table_b")
    table_c = create_mock_table("table_c")
    table_d = create_mock_table("table_d")

    # Setup: Group0: [A, B], Group1: [C, D]
    core_mocks_fx["get_sorted_tables"].return_value = [[table_a, table_b], [table_c, table_d]]
    
    # Simulate sequential execution by _process_table_data directly
    processed_order = []
    def sequential_process_table_data(table, *args, **kwargs):
        processed_order.append(table.name)
        return table.name # Simulate successful processing
    core_mocks_fx["process_table_data"].side_effect = sequential_process_table_data
    
    # To ensure that the test correctly reflects the order of processing due to group separation,
    # we need to make sure that `as_completed` yields futures in the order they would complete
    # if processed sequentially per group. The current `mock_as_completed` in the fixture
    # yields all submitted futures in their submission order.
    # For this specific test, we rely on the fact that ThreadPoolExecutor for group 0 finishes
    # before the one for group 1 starts.

    settings = AppSettings(
        source_db=DBConfig(driver="postgresql", database="src"),
        target_db=DBConfig(driver="postgresql", database="tgt"),
    )
    
    dump_and_load(settings, "/fake/alembic")

    # Assertions
    core_mocks_fx["get_sorted_tables"].assert_called_once_with(core_mocks_fx["metadata"])
    
    # Check that _process_table_data was called in the correct order
    expected_call_order = [
        call(table_a, ANY, ANY, ANY, ANY),
        call(table_b, ANY, ANY, ANY, ANY),
        call(table_c, ANY, ANY, ANY, ANY),
        call(table_d, ANY, ANY, ANY, ANY),
    ]
    core_mocks_fx["process_table_data"].assert_has_calls(expected_call_order, any_order=False)
    assert processed_order == ["table_a", "table_b", "table_c", "table_d"]

    # Check ThreadPoolExecutor calls: one per group
    assert core_mocks_fx["thread_pool_executor_cls"].call_count == 2
    # Check max_workers for each group
    args_list = core_mocks_fx["thread_pool_executor_cls"].call_args_list
    assert args_list[0][1]["max_workers"] == 2 # Group 0: [A, B]
    assert args_list[1][1]["max_workers"] == 2 # Group 1: [C, D]


def test_dump_and_load_parallel_within_group(core_mocks_fx):
    """
    Tests that all tables within a single dependency group are submitted to the
    ThreadPoolExecutor and that max_workers is set to the group size.
    """
    table_x = create_mock_table("table_x")
    table_y = create_mock_table("table_y")
    table_z = create_mock_table("table_z")

    # Setup: A single group with 3 tables
    core_mocks_fx["get_sorted_tables"].return_value = [[table_x, table_y, table_z]]
    
    settings = AppSettings(
        source_db=DBConfig(driver="postgresql", database="src"),
        target_db=DBConfig(driver="postgresql", database="tgt"),
    )
    
    dump_and_load(settings, "/fake/alembic")

    # Assertions
    # Check ThreadPoolExecutor was created once for the group
    core_mocks_fx["thread_pool_executor_cls"].assert_called_once()
    # Check max_workers was set to the size of the group
    assert core_mocks_fx["thread_pool_executor_cls"].call_args[1]["max_workers"] == 3

    # Check that submit was called for all tables in the group
    executor_instance = core_mocks_fx["executor_instance"]
    assert executor_instance.submit.call_count == 3
    
    submitted_table_names = sorted([task['table_name'] for task in core_mocks_fx["submitted_tasks_info"]])
    assert submitted_table_names == ["table_x", "table_y", "table_z"]


def test_dump_and_load_error_stops_further_group_processing(core_mocks_fx, mocker):
    """
    Tests that an error in one table stops processing of that table's group
    and any subsequent groups.
    """
    table_g1_t1 = create_mock_table("g1_t1") # Group 1, Table 1
    table_g1_t2 = create_mock_table("g1_t2") # Group 1, Table 2 (this one will fail)
    table_g2_t1 = create_mock_table("g2_t1") # Group 2, Table 1

    core_mocks_fx["get_sorted_tables"].return_value = [[table_g1_t1, table_g1_t2], [table_g2_t1]]

    # Simulate error for table_g1_t2
    # We need to modify the future object for table_g1_t2 to raise an exception
    # The mock_submit_fn in the fixture creates futures. We can alter its behavior for this test.
    
    original_submit_fn = core_mocks_fx["executor_instance"].submit.side_effect
    processed_tables_before_error = []

    def custom_submit_for_error_test(func, table_obj, *args, **kwargs):
        processed_tables_before_error.append(table_obj.name)
        future = original_submit_fn(func, table_obj, *args, **kwargs) # Get the standard future
        if table_obj.name == "g1_t2":
            # Configure this specific future to raise an error
            error_to_raise = ValueError(f"Failure processing {table_obj.name}")
            future.result = MagicMock(side_effect=error_to_raise)
            future.exception_value = error_to_raise # Store it for as_completed if it checks
        return future

    core_mocks_fx["executor_instance"].submit.side_effect = custom_submit_for_error_test
    
    settings = AppSettings(
        source_db=DBConfig(driver="postgresql", database="src"),
        target_db=DBConfig(driver="postgresql", database="tgt"),
    )
    
    with pytest.raises(Exception, match="Data migration failed for one or more tables."):
        dump_and_load(settings, "/fake/alembic")

    # Assertions
    # Check which tables were attempted (i.e., submitted)
    # Group 1: g1_t1, g1_t2. Group 2: g2_t1
    # g1_t1 should be submitted. g1_t2 should be submitted and fail.
    # g2_t1 should NOT be submitted because the first group failed.
    assert "g1_t1" in processed_tables_before_error
    assert "g1_t2" in processed_tables_before_error
    assert "g2_t1" not in processed_tables_before_error 
    
    # Verify that _process_table_data (the actual function, not the submit mock) was called for g1_t1
    # but its call for g1_t2 (which raises error) might not register "successfully" depending on mock setup.
    # The key is that `processed_tables_before_error` shows submission attempts.
    # The `process_table_data` mock from the fixture will still be called by the submit mechanism.
    # If g1_t1's future completes before g1_t2's error is handled, g1_t1's _process_table_data runs.
    # If g1_t2's future is checked first and errors, g1_t1's _process_table_data might be cancelled.
    # Given as_completed yields in submission order here:
    #   - g1_t1 future is processed, call to _process_table_data(g1_t1) happens.
    #   - g1_t2 future is processed, its .result() raises error.
    
    # Check calls to the actual _process_table_data mock
    # This mock is what's inside the 'executor.submit(mock_process_table_data, ...)'
    # So, if submit was called for g1_t1 and g1_t2, then _process_table_data was also called for them.
    assert core_mocks_fx["process_table_data"].call_count >= 1 # At least g1_t1 started
    
    # Ensure the executor for the second group was not even created/entered
    # The first group's executor:
    core_mocks_fx["thread_pool_executor_cls"].assert_called_once() 
    assert core_mocks_fx["thread_pool_executor_cls"].call_args[1]["max_workers"] == 2


# --- Existing module-level tests (ensure they are pytest compatible) ---
def test_chunk_iterable(): # Was already pytest style
    data = list(range(7))
    chunks = list(chunk_iterable(data, 3))
    assert chunks == [[0, 1, 2], [3, 4, 5], [6]]

# This test was for utils.get_sorted_tables, not core.get_sorted_tables which is mocked.
# It should ideally be in test_utils.py. If kept here, ensure it calls the right function.
# For this refactoring, assuming it's either moved or handled.
# If it's meant to test the real get_sorted_tables from utils:
def test_get_sorted_tables_and_circular_module_level_actual_util():
    from alembic_dump.utils import get_sorted_tables as actual_get_sorted_tables_util
    metadata = MetaData()
    parent = SATable("parent_util", metadata, Column("id", Integer, primary_key=True))
    SATable(
        "child_util",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("parent_id", Integer, ForeignKey(parent.c.id)), # Corrected FK syntax
    )
    # actual_get_sorted_tables_util now returns list[list[Table]]
    table_groups = actual_get_sorted_tables_util(metadata)
    assert len(table_groups) == 2 # [[parent_util], [child_util]]
    assert table_groups[0][0].name == "parent_util"
    assert table_groups[1][0].name == "child_util"
    
    cycles = detect_circular_dependencies(metadata)
    assert cycles == []

# Remove if __name__ == "__main__": unittest.main() was present. Pytest handles execution.The `tests/test_core.py` file has been refactored to use pytest conventions and updated to test the new group-based processing in `dump_and_load`.

**Summary of Changes for `tests/test_core.py`:**

1.  **Removed `unittest.TestCase`**: The class `TestCoreDumpAndLoadPrioritization` was removed.
2.  **Pytest Fixture `core_mocks_fx(mocker)`**:
    *   A comprehensive fixture was created to provide mocks for all core dependencies of `dump_and_load`:
        *   `create_db_manager` (and mock `from_db_manager`, `to_db_manager`, `metadata`).
        *   `sync_schema`.
        *   `alembic_dump.core.get_sorted_tables` (the one imported and used by `dump_and_load`).
        *   `alembic_dump.core._process_table_data`.
        *   `concurrent.futures.ThreadPoolExecutor` class and its instance.
    *   The fixture configures the mock executor's `submit` method to store submitted task details and allows futures to be controlled/inspected.
    *   It also mocks `concurrent.futures.as_completed` to yield futures in a deterministic order for testing.
3.  **Mock Table Factory `create_mock_table(name)`**: A helper function to create `MagicMock` instances for `Table` objects, primarily setting the `name` attribute.
4.  **Updated Test Cases for `dump_and_load`**:
    *   `test_dump_and_load_processes_groups_sequentially(core_mocks_fx, mocker)`:
        *   Mocks `get_sorted_tables` to return multiple groups (e.g., `[[A, B], [C, D]]`).
        *   Uses a side effect on `core_mocks_fx["process_table_data"]` to record the call order of actual table processing.
        *   Asserts that tables in group 0 are processed before tables in group 1.
        *   Asserts that `ThreadPoolExecutor` is created once per group with `max_workers` set to the size of that group.
    *   `test_dump_and_load_parallel_within_group(core_mocks_fx)`:
        *   Mocks `get_sorted_tables` to return a single group with multiple tables (e.g., `[[X, Y, Z]]`).
        *   Asserts that `ThreadPoolExecutor` is created once with `max_workers` equal to the number of tables in the group.
        *   Asserts that `executor.submit` was called for all tables in that group.
    *   `test_dump_and_load_error_stops_further_group_processing(core_mocks_fx, mocker)`:
        *   Mocks `get_sorted_tables` to return multiple groups.
        *   Configures the mock executor's `submit` logic to make a future for a specific table (e.g., `g1_t2` in group 0) raise an exception.
        *   Asserts that `dump_and_load` raises the expected "Data migration failed" exception.
        *   Asserts that tables processed before the error were submitted, but tables in subsequent groups (e.g., `g2_t1`) were not.
        *   Asserts that the `ThreadPoolExecutor` was only created for the group(s) that started processing.
5.  **Removed Old Tests**: Tests related to `settings.table_priorities` and `settings.max_parallel_tasks` were removed as that functionality is no longer part of `dump_and_load`.
6.  **Existing Tests**: Module-level tests like `test_chunk_iterable` were kept. The `test_get_sorted_tables_and_circular_module_level` was updated to `test_get_sorted_tables_and_circular_module_level_actual_util` to explicitly test the actual `get_sorted_tables` from `alembic_dump.utils` and reflect its new return type (`list[list[Table]]`).

Both `tests/test_utils.py` and `tests/test_core.py` have now been refactored according to the subtask requirements.
