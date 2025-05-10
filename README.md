# Alembic Dump

A Python library to dump, load, and mask data between databases (e.g., PostgreSQL, MySQL) in environments managed by Alembic, with SSH tunnel support.

This tool is designed to help developers synchronize database schemas using Alembic revisions and then transfer data, optionally applying masking rules to sensitive information. It's particularly useful for creating staging or development environments from production data, or for migrating data between different database instances while maintaining schema integrity.

## Key Features

* **Schema Synchronization**: Ensures target database schema matches the source database schema based on Alembic revisions before data transfer.
* **Data Dump & Load**: Efficiently transfers data table by table, respecting foreign key constraints by processing tables in a topologically sorted order.
* **Data Masking**: Supports various strategies (e.g., hashing, partial masking, using Faker) to anonymize sensitive data during the transfer. Masking rules can be configured per table and per column.
* **SSH Tunneling**: Built-in support for connecting to databases via an SSH bastion host.
* **Configuration**: Uses Pydantic for clear and validated configuration of database connections, SSH tunnels, and masking rules.
* **Chunking**: Processes data in chunks to manage memory usage effectively, especially for large tables.
* **Flexible Table Selection**: Allows specifying tables to include or exclude from the dump/load process.

## Installation

You can install `alembic-dump` using `pip`:

```bash
pip install alembic-dump
```

baisc usage 
```python3
from alembic_dump.config import AppSettings, DBConfig, MaskingConfig, MaskingRule
from alembic_dump.core import dump_and_load

# 1. Define Source and Target Database Configurations
source_db_config = DBConfig(
    driver="postgresql",
    host="source.db.example.com",
    port=5432,
    username="user",
    password="password", # In real use, manage secrets carefully (e.g., env vars)
    database="sourcedb"
)

target_db_config = DBConfig(
    driver="postgresql",
    host="target.db.example.com",
    port=5432,
    username="user",
    password="password",
    database="targetdb"
)

# 2. (Optional) Define Masking Rules
masking_config = MaskingConfig(
    rules={
        "users": {
            "email": MaskingRule(strategy="hash"),
            "full_name": MaskingRule(strategy="faker", faker_provider="name")
        },
        "sensitive_logs": {
            "ip_address": MaskingRule(strategy="null")
        }
    }
)

# 3. (Optional) Define SSH Tunnel Configuration if needed
# ssh_config = SSHConfig(...)

# 4. Create AppSettings
app_settings = AppSettings(
    source_db=source_db_config,
    target_db=target_db_config,
    # ssh_tunnel=ssh_config, # Uncomment if using SSH tunnel
    masking=masking_config,
    chunk_size=1000,
    tables_to_exclude=["some_large_irrelevant_table"],
    # tables_to_include=["users", "orders"] # Only include these if specified
)

# 5. Specify the path to your Alembic migrations directory
# This directory should contain your alembic.ini and version scripts.
alembic_migrations_directory = "/path/to/your/alembic_migrations" 
# For testing, you might use a dedicated test Alembic environment.

# 6. Run the dump and load process
try:
    dump_and_load(settings=app_settings, alembic_dir=alembic_migrations_directory)
    print("Data dump and load completed successfully!")
except Exception as e:
    print(f"An error occurred: {e}")
```