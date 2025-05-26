import pytest
from unittest.mock import patch # Keep for mocking if mocker fixture is not used directly for property mock

from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table

from alembic_dump.utils import detect_circular_dependencies, get_sorted_tables


# Pytest fixture for a common table schema
@pytest.fixture
def table_schema_fx():
    """Provides a MetaData object and several Table objects with defined dependencies."""
    metadata = MetaData()
    
    # Level 0 (no dependencies)
    categories_table = Table(
        "categories", metadata, Column("id", Integer, primary_key=True), Column("name", String)
    )
    products_table = Table(
        "products", metadata, Column("id", Integer, primary_key=True), Column("name", String), Column("category_id", Integer, ForeignKey("categories.id"))
    )
    users_table = Table(
        "users", metadata, Column("id", Integer, primary_key=True), Column("name", String)
    )

    # Level 1
    user_profiles_table = Table(
        "user_profiles", metadata, Column("id", Integer, primary_key=True), Column("user_id", Integer, ForeignKey("users.id"))
    )
    orders_table = Table(
        "orders", metadata, Column("id", Integer, primary_key=True), Column("user_id", Integer, ForeignKey("users.id"))
    )
    
    # Level 2
    order_items_table = Table(
        "order_items", metadata, Column("id", Integer, primary_key=True), Column("order_id", Integer, ForeignKey("orders.id")), Column("product_id", Integer, ForeignKey("products.id"))
    )
    
    # To make sure SQLAlchemy registers dependencies correctly for its internal .sorted_tables (used in fallback)
    metadata.sorted_tables 
    
    return {
        "metadata": metadata,
        "categories_table": categories_table,
        "products_table": products_table,
        "users_table": users_table,
        "user_profiles_table": user_profiles_table,
        "orders_table": orders_table,
        "order_items_table": order_items_table,
    }

def get_table_names_from_groups(groups: list[list[Table]]) -> list[list[str]]:
    """Helper to extract table names from groups of Table objects."""
    return [[table.name for table in group] for group in groups]

def test_get_sorted_tables_groups_by_dependency(table_schema_fx):
    """
    Test that get_sorted_tables groups tables correctly by dependency level
    and sorts them alphabetically within each group.
    """
    metadata = table_schema_fx["metadata"]
    table_groups = get_sorted_tables(metadata)
    
    assert isinstance(table_groups, list), "Should return a list"
    for group in table_groups:
        assert isinstance(group, list), "Each item in the main list should be a list (a group)"
        for item in group:
            assert isinstance(item, Table), "Each item in a group should be a Table object"

    names_in_groups = get_table_names_from_groups(table_groups)
    
    # Expected grouping based on dependencies (and alphabetical within groups):
    # Level 0: categories, users (independent)
    # Level 1: orders, products, user_profiles (depend on Level 0)
    #          - products depends on categories
    #          - orders depends on users
    #          - user_profiles depends on users
    # Level 2: order_items (depends on orders and products)

    # Actual expected groups:
    # Group 0: 'categories', 'users' (alphabetical)
    # Group 1: 'orders', 'products', 'user_profiles' (alphabetical; products depends on categories, orders/user_profiles on users)
    # Group 2: 'order_items' (depends on orders, products)
    
    expected_groups = [
        ["categories", "users"],  # Level 0, sorted alphabetically
        ["orders", "products", "user_profiles"], # Level 1, sorted alphabetically
        ["order_items"], # Level 2
    ]
    
    assert names_in_groups == expected_groups, \
        f"Table groups do not match expected. Got: {names_in_groups}, Expected: {expected_groups}"

    # Verify all tables are present
    all_tables_in_result = [name for group in names_in_groups for name in group]
    all_defined_tables = ["categories", "products", "users", "user_profiles", "orders", "order_items"]
    assert sorted(all_tables_in_result) == sorted(all_defined_tables), "Not all tables were present in the result"


def test_get_sorted_tables_empty_metadata():
    """Test with empty metadata."""
    metadata = MetaData()
    table_groups = get_sorted_tables(metadata)
    assert table_groups == [], "Should return an empty list for empty metadata"


def test_get_sorted_tables_no_dependencies(table_schema_fx):
    """Test with tables that have no FK dependencies among themselves."""
    metadata = MetaData()
    Table("t1_alpha", metadata, Column("id", Integer, primary_key=True))
    Table("t3_gamma", metadata, Column("id", Integer, primary_key=True))
    Table("t2_beta", metadata, Column("id", Integer, primary_key=True))
    
    table_groups = get_sorted_tables(metadata)
    names_in_groups = get_table_names_from_groups(table_groups)
    
    # All in one group, sorted alphabetically
    expected_groups = [["t1_alpha", "t2_beta", "t3_gamma"]]
    assert names_in_groups == expected_groups


@patch("sqlalchemy.MetaData.sorted_tables", new_callable=unittest.mock.PropertyMock)
def test_get_sorted_tables_fallback_on_error(mock_sqlalchemy_sorted_tables, table_schema_fx, caplog):
    """
    Test the fallback mechanism of get_sorted_tables when an internal error occurs
    during graph construction or processing.
    It should fall back to using metadata.sorted_tables in a single group.
    """
    metadata = table_schema_fx["metadata"]
    
    # Simulate an error during the custom graph processing part of get_sorted_tables.
    # We can achieve this by making table.foreign_keys raise an error for an unrelated table
    # that might be processed during the graph build.
    # A more direct way is to patch a part of the graph building in get_sorted_tables if possible,
    # or simulate a condition that leads to its internal fallback.
    # For now, let's test the case where metadata.sorted_tables itself fails,
    # as this is one of the explicit fallback paths.
    
    mock_sqlalchemy_sorted_tables.side_effect = Exception("Simulated SQLAlchemy internal sort error")
    
    table_groups = get_sorted_tables(metadata) # This should trigger the fallback
    
    # The fallback logic is:
    # 1. try: return [list(metadata.sorted_tables)] -> this will fail due to mock
    # 2. except: logger.error(...)
    # 3. return [list(metadata.tables.values())] -> this is the expected fallback path now
    
    assert "Fallback to metadata.sorted_tables also failed" in caplog.text # Check log
    
    assert isinstance(table_groups, list)
    assert len(table_groups) == 1, "Fallback should produce a single group"
    
    # The order within this single group will be arbitrary from metadata.tables.values()
    # So, just check if all tables are present.
    fallback_table_names = sorted([t.name for t in table_groups[0]])
    all_defined_table_names = sorted([
        table_schema_fx["categories_table"].name,
        table_schema_fx["products_table"].name,
        table_schema_fx["users_table"].name,
        table_schema_fx["user_profiles_table"].name,
        table_schema_fx["orders_table"].name,
        table_schema_fx["order_items_table"].name,
    ])
    assert fallback_table_names == all_defined_table_names, \
        "Fallback group does not contain all expected tables"

# --- Tests for detect_circular_dependencies (converted to pytest style) ---

def test_no_circular_dependencies():
    metadata = MetaData()
    Table("table_a", metadata, Column("id", Integer, primary_key=True))
    Table(
        "table_b",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("a_id", Integer, ForeignKey("table_a.id")),
    )
    Table(
        "table_c",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("b_id", Integer, ForeignKey("table_b.id")),
    )
    assert detect_circular_dependencies(metadata) == []

def test_circular_dependencies():
    metadata = MetaData()
    # Table A depends on Table B
    Table(
        "table_a",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("b_id", Integer, ForeignKey("table_b.id")), # type: ignore # Incomplete schema for test
    )
    # Table B depends on Table A
    Table(
        "table_b",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("a_id", Integer, ForeignKey("table_a.id")), # type: ignore # Incomplete schema for test
    )
    
    # To make this a valid circular dependency that SQLAlchemy can handle for sorted_tables:
    # We need to ensure the tables are "known" to metadata before creating FKs to each other.
    # However, detect_circular_dependencies builds its own graph.
    
    cycles = detect_circular_dependencies(metadata)
    assert len(cycles) == 1
    # Sort items in the set for deterministic comparison
    assert sorted(list(cycles[0])) == sorted(["table_a", "table_b"])

def test_complex_circular_dependencies():
    metadata = MetaData()
    Table("t1", metadata, Column("id", Integer, primary_key=True), Column("t2_id", Integer, ForeignKey("t2.id"))) # type: ignore
    Table("t2", metadata, Column("id", Integer, primary_key=True), Column("t3_id", Integer, ForeignKey("t3.id"))) # type: ignore
    Table("t3", metadata, Column("id", Integer, primary_key=True), Column("t1_id", Integer, ForeignKey("t1.id"))) # type: ignore
    Table("t4", metadata, Column("id", Integer, primary_key=True), Column("t1_id", Integer, ForeignKey("t1.id"))) # type: ignore
    Table("t5", metadata, Column("id", Integer, primary_key=True)) # Independent

    cycles = detect_circular_dependencies(metadata)
    assert len(cycles) == 1
    assert sorted(list(cycles[0])) == sorted(["t1", "t2", "t3"])

def test_self_referential_foreign_key_not_a_cycle_for_detect(table_schema_fx):
    """Self-referential FKs are not considered cycles by detect_circular_dependencies."""
    metadata = MetaData()
    Table(
        "employee",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("manager_id", Integer, ForeignKey("employee.id")), # Self-reference
    )
    assert detect_circular_dependencies(metadata) == []

# Ensure to remove if __name__ == "__main__": unittest.main() if it exists
# Pytest handles test discovery and execution.The `tests/test_utils.py` file has been refactored to use pytest conventions.

**Summary of Changes for `tests/test_utils.py`:**

1.  **Removed `unittest.TestCase`**: The class `TestGetSortedTables` was removed.
2.  **Pytest Fixture `table_schema_fx`**:
    *   A fixture named `table_schema_fx` was created.
    *   It sets up a `MetaData` object and several `Table` objects (`categories`, `products`, `users`, `user_profiles`, `orders`, `order_items`) with inter-dependencies to create multiple dependency levels.
3.  **Updated Tests for `get_sorted_tables`**:
    *   `test_get_sorted_tables_groups_by_dependency(table_schema_fx)`:
        *   Uses the `table_schema_fx` fixture.
        *   Asserts that `get_sorted_tables` returns a `list[list[Table]]`.
        *   Verifies the correct grouping of tables by dependency levels.
        *   Verifies that tables within each group are sorted alphabetically by name. The expected groups are:
            *   Group 0: `['categories', 'users']`
            *   Group 1: `['orders', 'products', 'user_profiles']`
            *   Group 2: `['order_items']`
    *   `test_get_sorted_tables_empty_metadata()`: Checks behavior with empty metadata.
    *   `test_get_sorted_tables_no_dependencies()`: Checks behavior when tables have no FKs (all should be in one group, sorted alphabetically).
    *   `test_get_sorted_tables_fallback_on_error(mock_sqlalchemy_sorted_tables, table_schema_fx, caplog)`:
        *   Uses `unittest.mock.patch` (imported as `patch`) to mock `sqlalchemy.MetaData.sorted_tables` to simulate an error during one of the fallback paths.
        *   Verifies that an appropriate warning is logged (using `caplog`).
        *   Asserts that the function falls back to returning a single list of all tables (specifically, the `[list(metadata.tables.values())]` path when `metadata.sorted_tables` also fails or is the source of the primary error).
    *   Removed tests related to the old `table_priorities` argument as it no longer exists.
4.  **`detect_circular_dependencies` Tests**:
    *   Converted to pytest style (they were already functions, so minor changes like ensuring `assert` statements are used directly).
    *   Added `test_complex_circular_dependencies` and `test_self_referential_foreign_key_not_a_cycle_for_detect`.

Now, I will proceed to refactor `tests/test_core.py`.
