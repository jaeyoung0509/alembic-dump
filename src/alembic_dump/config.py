from typing import Any, Optional

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SSHConfig(BaseModel):
    host: str
    port: int = 22
    username: str
    password: Optional[SecretStr] = None
    private_key_path: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "host": "bastion.example.com",
                    "port": 22,
                    "username": "ssh_user",
                    "private_key_path": "~/.ssh/id_rsa"
                }
            ]
        }
    }

class DBConfig(BaseModel):
    driver: str = Field(
        description="Database driver (e.g., postgresql, mysql)",
        examples=["postgresql", "mysql"]
    )
    host: str
    port: Optional[int] = None
    username: str
    password: SecretStr
    database: str
    options: dict[str, Any] = Field(default_factory=dict)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "driver": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "username": "db_user",
                    "password": "secret",
                    "database": "my_db"
                }
            ]
        }
    }

class MaskingRule(BaseModel):
    strategy: str = Field(
        description="Masking strategy to apply",
        examples=["hash", "faker", "null", "partial", "custom"]
    )
    faker_provider: Optional[str] = Field(
        None,
        description="Faker provider name (e.g., 'name', 'email')",
        examples=["name", "email", "phone_number"]
    )
    hash_salt: Optional[str] = Field(
        None,
        description="Salt for hash-based masking"
    )
    partial_keep_chars: int = Field(
        default=4,
        description="Number of characters to keep in partial masking"
    )
    custom_function: Optional[str] = Field(
        None,
        description="Path to custom masking function"
    )

class MaskingConfig(BaseModel):
    rules: dict[str, dict[str, MaskingRule]] = Field(
        description="Masking rules by table and column",
        examples=[
            {
                "users": {
                    "email": {"strategy": "hash"},
                    "name": {"strategy": "faker", "faker_provider": "name"}
                }
            }
        ]
    )
    default_salt: Optional[str] = Field(
        None,
        description="Default salt for hash-based masking"
    )

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_prefix='ALEMBIC_DUMP_',
        env_nested_delimiter='__',
        case_sensitive=False,
        extra='ignore',
        json_schema_extra={
            "examples": [
                {
                    "source_db": {
                        "driver": "postgresql",
                        "host": "source.example.com",
                        "port": 5432,
                        "username": "source_user",
                        "password": "source_pass",
                        "database": "source_db"
                    },
                    "target_db": {
                        "driver": "postgresql",
                        "host": "target.example.com",
                        "port": 5432,
                        "username": "target_user",
                        "password": "target_pass",
                        "database": "target_db"
                    },
                    "ssh_tunnel": {
                        "host": "bastion.example.com",
                        "port": 22,
                        "username": "ssh_user",
                        "private_key_path": "~/.ssh/id_rsa"
                    },
                    "masking": {
                        "rules": {
                            "users": {
                                "email": {"strategy": "hash"},
                                "name": {"strategy": "faker", "faker_provider": "name"}
                            }
                        },
                        "default_salt": "my-secret-salt"
                    },
                    "chunk_size": 1000
                }
            ]
        }
    )

    source_db: DBConfig
    target_db: DBConfig
    ssh_tunnel: Optional[SSHConfig] = None
    masking: Optional[MaskingConfig] = None
    tables_to_include: Optional[list[str]] = None
    tables_to_exclude: Optional[list[str]] = None
    chunk_size: int = Field(
        default=1000,
        description="Number of records to process in each chunk"
    )
