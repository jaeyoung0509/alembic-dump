from typing import Any, Optional, Union

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import TypedDict

from .secrets import AWSSecretsManagerConfig, HashiCorpVaultConfig


class SecretKeyMapping(TypedDict, total=False):
    """Mapping of DBConfig fields to secret keys"""

    username: str
    password: str
    host: str
    port: str
    database: str


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
                    "private_key_path": "~/.ssh/id_rsa",
                }
            ]
        }
    }


class DBConfig(BaseModel):
    driver: str = Field(
        description="Database driver (e.g., postgresql)",
        examples=["postgresql"],
    )
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    database: Optional[str] = None
    options: dict[str, Any] = Field(default_factory=dict)

    # Secret provider configuration
    secret_provider_config: Optional[
        Union[AWSSecretsManagerConfig, HashiCorpVaultConfig]
    ] = None
    # Mapping of DBConfig fields to secret keys
    secret_key_mapping: Optional[SecretKeyMapping] = Field(
        None,
        description="Mapping of DBConfig fields to secret keys (e.g., {'username': 'db_user', 'password': 'db_pass'})",
    )

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._populate_from_secret_provider()

    def _populate_from_secret_provider(self) -> None:
        """Populate fields from secret provider if configured"""
        if self.secret_provider_config and self.secret_key_mapping:
            from .secrets import create_secret_provider

            provider = create_secret_provider(self.secret_provider_config)

            for field, secret_key in self.secret_key_mapping.items():
                if not isinstance(secret_key, str):
                    continue
                value = provider.get_secret_value(secret_key)
                if value is not None:
                    if field == "password":
                        setattr(self, field, SecretStr(value))
                    elif field == "port":
                        setattr(self, field, int(value))
                    else:
                        setattr(self, field, value)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "driver": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "username": "db_user",
                    "password": "secret",
                    "database": "my_db",
                },
                {
                    "driver": "postgresql",
                    "database": "my_db",
                    "secret_provider_config": {
                        "provider_type": "aws_secrets_manager",
                        "secret_id": "arn:aws:secretsmanager:region:account:secret:db-credentials",
                        "region_name": "us-west-2",
                    },
                    "secret_key_mapping": {
                        "host": "db_host",
                        "port": "db_port",
                        "username": "db_user",
                        "password": "db_pass",
                    },
                },
            ]
        }
    }


class MaskingRule(BaseModel):
    strategy: str = Field(
        description="Masking strategy to apply",
        examples=["hash", "faker", "null", "partial", "custom"],
    )
    faker_provider: Optional[str] = Field(
        None,
        description="Faker provider name (e.g., 'name', 'email')",
        examples=["name", "email", "phone_number"],
    )
    hash_salt: Optional[str] = Field(None, description="Salt for hash-based masking")
    partial_keep_chars: int = Field(
        default=4, description="Number of characters to keep in partial masking"
    )
    custom_function: Optional[str] = Field(
        None, description="Path to custom masking function"
    )


class MaskingConfig(BaseModel):
    rules: dict[str, dict[str, MaskingRule]] = Field(
        description="Masking rules by table and column",
        examples=[
            {
                "users": {
                    "email": {"strategy": "hash"},
                    "name": {"strategy": "faker", "faker_provider": "name"},
                }
            }
        ],
    )
    default_salt: Optional[str] = Field(
        None, description="Default salt for hash-based masking"
    )


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ALEMBIC_DUMP_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
        json_schema_extra={
            "examples": [
                {
                    "source_db": {
                        "driver": "postgresql",
                        "host": "source.example.com",
                        "port": 5432,
                        "username": "source_user",
                        "password": "source_pass",
                        "database": "source_db",
                    },
                    "target_db": {
                        "driver": "postgresql",
                        "host": "target.example.com",
                        "port": 5432,
                        "username": "target_user",
                        "password": "target_pass",
                        "database": "target_db",
                    },
                    "source_ssh_tunnel": {
                        "host": "bastion-source.example.com",
                        "port": 22,
                        "username": "ssh_user_source",
                        "private_key_path": "~/.ssh/id_rsa_source",
                    },
                    "target_ssh_tunnel": {
                        "host": "bastion-target.example.com",
                        "port": 2222,  # 다른 포트 예시
                        "username": "ssh_user_target",
                        "private_key_path": "~/.ssh/id_rsa_target",
                    },
                    "masking": {
                        "rules": {
                            "users": {
                                "email": {"strategy": "hash"},
                                "name": {"strategy": "faker", "faker_provider": "name"},
                            }
                        },
                        "default_salt": "my-secret-salt",
                    },
                    "chunk_size": 1000,
                }
            ]
        },
    )

    source_db: DBConfig
    target_db: DBConfig
    source_ssh_tunnel: Optional[SSHConfig] = None
    target_ssh_tunnel: Optional[SSHConfig] = None
    masking: Optional[MaskingConfig] = None
    tables_to_include: Optional[list[str]] = None
    tables_to_exclude: Optional[list[str]] = None
    chunk_size: int = Field(
        default=100000, description="Number of records to process in each chunk"
    )
