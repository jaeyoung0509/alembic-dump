import json
from contextlib import suppress
from typing import Any

import boto3
import hvac
import pytest
from mypy_boto3_secretsmanager import SecretsManagerClient
from pydantic import SecretStr

from alembic_dump.config import AppSettings, DBConfig
from alembic_dump.secrets import (
    AWSSecretsManagerConfig,
    HashiCorpVaultConfig,
    create_secret_provider,
)


@pytest.fixture
def aws_secrets_manager_config(
    localstack_container: dict[str, Any],
) -> AWSSecretsManagerConfig:
    """Create AWS Secrets Manager configuration for testing"""
    return AWSSecretsManagerConfig(
        provider_type="aws_secrets_manager",
        secret_id="test-secret",
        endpoint_url=localstack_container["endpoint_url"],
        region_name="us-east-1",
        role_arn=None,
        profile_name=None,
    )


@pytest.fixture
def hashicorp_vault_config(vault_container: dict[str, Any]) -> HashiCorpVaultConfig:
    """Create HashiCorp Vault configuration for testing"""
    return HashiCorpVaultConfig(
        provider_type="hashicorp_vault",
        vault_addr=vault_container["url"],
        secret_path="db-credentials",
        vault_token=SecretStr(vault_container["token"]),
        role_id=None,
        secret_id=None,
    )


@pytest.fixture
def aws_secrets_manager_client(
    localstack_container: dict[str, Any],
) -> SecretsManagerClient:
    """Create AWS Secrets Manager client for testing"""
    session = boto3.session.Session()
    return session.client(
        "secretsmanager",
        endpoint_url=localstack_container["endpoint_url"],
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def vault_client(vault_container: dict[str, Any]) -> hvac.Client:
    """Create HashiCorp Vault client for testing"""
    client = hvac.Client(url=vault_container["url"], token=vault_container["token"])
    return client


def test_aws_secrets_manager_provider(
    aws_secrets_manager_config: AWSSecretsManagerConfig,
    aws_secrets_manager_client: SecretsManagerClient,
) -> None:
    """Test AWS Secrets Manager provider"""
    with suppress(aws_secrets_manager_client.exceptions.ResourceNotFoundException):
        # Clean up secret if it exists
        aws_secrets_manager_client.delete_secret(
            SecretId=aws_secrets_manager_config.secret_id,
            ForceDeleteWithoutRecovery=True,
        )

    # Create test secret
    test_secret = {
        "username": "test_user",
        "password": "test_password",
        "host": "test_host",
        "port": "5432",
    }
    aws_secrets_manager_client.create_secret(
        Name=aws_secrets_manager_config.secret_id,
        SecretString=json.dumps(test_secret),
    )

    # Create provider and test
    provider = create_secret_provider(aws_secrets_manager_config)
    assert provider.get_secret_value("username") == "test_user"


def test_hashicorp_vault_provider(
    hashicorp_vault_config: HashiCorpVaultConfig,
    vault_client: hvac.Client,
) -> None:
    """Test HashiCorp Vault provider"""
    # Clean up secret if it exists
    with suppress(vault_client.secrets.kv.v2.delete_metadata_and_all_versions):
        vault_client.secrets.kv.v2.delete_metadata_and_all_versions(
            path=hashicorp_vault_config.secret_path
        )

    # Create test secret
    test_secret = {
        "username": "test_user",
        "password": "test_password",
        "host": "test_host",
        "port": "5432",
    }
    vault_client.secrets.kv.v2.create_or_update_secret(
        path=hashicorp_vault_config.secret_path,
        secret=test_secret,
    )

    # Create provider and test
    provider = create_secret_provider(hashicorp_vault_config)
    assert provider.get_secret_value("username") == "test_user"
    assert provider.get_secret_value("password") == "test_password"


def test_db_config_with_aws_secrets(
    aws_secrets_manager_config: AWSSecretsManagerConfig,
    aws_secrets_manager_client: SecretsManagerClient,
) -> None:
    """Test DBConfig with AWS Secrets Manager"""
    # Clean up secret if it exists
    with suppress(aws_secrets_manager_client.exceptions.ResourceNotFoundException):
        aws_secrets_manager_client.delete_secret(
            SecretId=aws_secrets_manager_config.secret_id,
            ForceDeleteWithoutRecovery=True,
        )

    # Create test secret
    test_secret = {
        "db_user": "test_user",
        "db_pass": "test_password",
        "db_host": "test_host",
        "db_port": "5432",
    }
    aws_secrets_manager_client.create_secret(
        Name=aws_secrets_manager_config.secret_id,
        SecretString=json.dumps(test_secret),
    )


def test_db_config_with_vault_secrets(
    hashicorp_vault_config: HashiCorpVaultConfig,
    vault_client: hvac.Client,
) -> None:
    """Test DBConfig with HashiCorp Vault"""
    # Clean up secret if it exists
    with suppress(vault_client.secrets.kv.v2.delete_metadata_and_all_versions):
        vault_client.secrets.kv.v2.delete_metadata_and_all_versions(
            path=hashicorp_vault_config.secret_path
        )

    # Create test secret
    test_secret = {
        "db_user": "test_user",
        "db_pass": "test_password",
        "db_host": "test_host",
        "db_port": "5432",
    }
    vault_client.secrets.kv.v2.create_or_update_secret(
        path=hashicorp_vault_config.secret_path,
        secret=test_secret,
    )

    # Test configuration values
    provider = create_secret_provider(hashicorp_vault_config)
    assert provider.get_secret_value("db_user") == "test_user"
    assert provider.get_secret_value("db_pass") == "test_password"
    assert provider.get_secret_value("db_host") == "test_host"
    assert provider.get_secret_value("db_port") == "5432"


def test_app_settings_with_secrets(
    aws_secrets_manager_config: AWSSecretsManagerConfig,
    aws_secrets_manager_client: SecretsManagerClient,
) -> None:
    """Test AppSettings with secret providers"""
    # Clean up existing secret if it exists
    with suppress(aws_secrets_manager_client.exceptions.ResourceNotFoundException):
        aws_secrets_manager_client.delete_secret(
            SecretId=aws_secrets_manager_config.secret_id,
            ForceDeleteWithoutRecovery=True,
        )

    # Create test secret
    test_secret = {
        "db_user": "test_user",
        "db_pass": "test_password",
        "db_host": "test_host",
        "db_port": "5432",
    }
    aws_secrets_manager_client.create_secret(
        Name=aws_secrets_manager_config.secret_id,
        SecretString=json.dumps(test_secret),
    )

    # Create AppSettings with secret provider
    settings = AppSettings(
        source_db=DBConfig(
            driver="postgresql",
            database="test_db",
            secret_provider_config=aws_secrets_manager_config,
            secret_key_mapping={
                "username": "db_user",
                "password": "db_pass",
                "host": "db_host",
                "port": "db_port",
            },
            password=SecretStr("test_password"),
        ),
        target_db=DBConfig(
            driver="postgresql",
            host="target.example.com",
            port=5432,
            username="target_user",
            password=SecretStr("target_pass"),
            database="target_db",
            secret_key_mapping=None,
        ),
    )

    # Test source database configuration
    assert settings.source_db.username == "test_user"
    assert settings.source_db.password.get_secret_value() == "test_password" #type: ignore
    assert settings.source_db.host == "test_host"
    assert settings.source_db.port == 5432

    # Test target database configuration (direct values)
    assert settings.target_db.username == "target_user"
    assert settings.target_db.password.get_secret_value() == "target_pass" #type: ignore
    assert settings.target_db.host == "target.example.com"
    assert settings.target_db.port == 5432
