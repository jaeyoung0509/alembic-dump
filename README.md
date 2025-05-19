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
* **External Secret Management**: Integration with AWS Secrets Manager and HashiCorp Vault for managing sensitive information.

## Installation

You can install `alembic-dump` using `pip`:

```bash
# Basic installation
pip install alembic-dump

# With AWS Secrets Manager support
pip install alembic-dump[aws]

# With HashiCorp Vault support
pip install alembic-dump[vault]

# With both secret management systems
pip install alembic-dump[aws,vault]

# For development (includes all optional dependencies)
pip install alembic-dump[dev]
```

## Quick Start

```python
from alembic_dump.core import dump_and_load
from alembic_dump.config import AppSettings

# Basic configuration
settings = AppSettings(
    source_db={
        "driver": "postgresql",
        "host": "source.example.com",
        "port": 5432,
        "username": "source_user",
        "password": "source_pass",
        "database": "source_db",
    },
    target_db={
        "driver": "postgresql",
        "host": "target.example.com",
        "port": 5432,
        "username": "target_user",
        "password": "target_pass",
        "database": "target_db",
    }
)

# Run the dump and load process
dump_and_load(settings, alembic_dir="path/to/alembic")
```

## Configuration

Configuration can be provided through environment variables, a `.env` file, or directly in code using the `AppSettings` class.

### Using Secret Management

#### AWS Secrets Manager

```python
from alembic_dump.config import AppSettings

settings = AppSettings(
    source_db={
        "driver": "postgresql",
        "database": "source_db",
        "secret_provider_config": {
            "provider_type": "aws_secrets_manager",
            "secret_id": "arn:aws:secretsmanager:region:account:secret:db-credentials",
            "region_name": "us-west-2",
            # Optional: role_arn for cross-account access
            # "role_arn": "arn:aws:iam::account:role/role-name",
            # Optional: profile_name for local AWS credentials
            # "profile_name": "my-profile"
        },
        "secret_key_mapping": {
            "host": "db_host",
            "port": "db_port",
            "username": "db_user",
            "password": "db_pass"
        }
    }
)
```

#### HashiCorp Vault

```python
from alembic_dump.config import AppSettings

settings = AppSettings(
    source_db={
        "driver": "postgresql",
        "database": "source_db",
        "secret_provider_config": {
            "provider_type": "hashicorp_vault",
            "vault_addr": "https://vault.example.com:8200",
            "secret_path": "secret/data/db-credentials",
            # Either vault_token or role_id/secret_id must be provided
            "vault_token": "s.token",
            # "role_id": "role-id",
            # "secret_id": "secret-id"
        },
        "secret_key_mapping": {
            "host": "db_host",
            "port": "db_port",
            "username": "db_user",
            "password": "db_pass"
        }
    }
)
```

### Environment Variables

All configuration can also be provided through environment variables:

```bash
# Basic configuration
ALEMBIC_DUMP_SOURCE_DB__DRIVER=postgresql
ALEMBIC_DUMP_SOURCE_DB__HOST=source.example.com
ALEMBIC_DUMP_SOURCE_DB__PORT=5432
ALEMBIC_DUMP_SOURCE_DB__USERNAME=source_user
ALEMBIC_DUMP_SOURCE_DB__PASSWORD=source_pass
ALEMBIC_DUMP_SOURCE_DB__DATABASE=source_db

# AWS Secrets Manager configuration
ALEMBIC_DUMP_SOURCE_DB__SECRET_PROVIDER_CONFIG__PROVIDER_TYPE=aws_secrets_manager
ALEMBIC_DUMP_SOURCE_DB__SECRET_PROVIDER_CONFIG__SECRET_ID=arn:aws:secretsmanager:region:account:secret:db-credentials
ALEMBIC_DUMP_SOURCE_DB__SECRET_PROVIDER_CONFIG__REGION_NAME=us-west-2
ALEMBIC_DUMP_SOURCE_DB__SECRET_KEY_MAPPING__HOST=db_host
ALEMBIC_DUMP_SOURCE_DB__SECRET_KEY_MAPPING__PORT=db_port
ALEMBIC_DUMP_SOURCE_DB__SECRET_KEY_MAPPING__USERNAME=db_user
ALEMBIC_DUMP_SOURCE_DB__SECRET_KEY_MAPPING__PASSWORD=db_pass

# HashiCorp Vault configuration
ALEMBIC_DUMP_SOURCE_DB__SECRET_PROVIDER_CONFIG__PROVIDER_TYPE=hashicorp_vault
ALEMBIC_DUMP_SOURCE_DB__SECRET_PROVIDER_CONFIG__VAULT_ADDR=https://vault.example.com:8200
ALEMBIC_DUMP_SOURCE_DB__SECRET_PROVIDER_CONFIG__SECRET_PATH=secret/data/db-credentials
ALEMBIC_DUMP_SOURCE_DB__SECRET_PROVIDER_CONFIG__VAULT_TOKEN=s.token
```

## Development

1. Clone the repository:
   ```bash
   git clone https://github.com/jaeyoung0509/alembic-dump.git
   cd alembic-dump
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

4. Run tests:
   ```bash
   pytest
   ```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT