import logging
from typing import Optional, Protocol, Union

import boto3
import hvac
from botocore.config import Config
from mypy_boto3_secretsmanager import SecretsManagerClient
from pydantic import BaseModel, Field, SecretStr

logger = logging.getLogger(__name__)


# --- Base Secret Provider Protocol ---
class SecretProvider(Protocol):
    """Protocol defining the interface for secret providers"""

    def get_secret_value(self, key_name: str) -> Optional[str]:
        """Get a specific secret value by key name"""
        ...

    def get_all_secrets(self) -> dict[str, Optional[str]]:
        """Get all secrets as a dictionary"""
        ...


# --- AWS Secrets Manager ---
class AWSSecretsManagerConfig(BaseModel):
    """Configuration for AWS Secrets Manager"""

    provider_type: str = "aws_secrets_manager"
    secret_id: str = Field(description="The ARN or name of the secret")
    region_name: Optional[str] = Field(None, description="AWS region name")
    role_arn: Optional[str] = Field(None, description="IAM role ARN to assume")
    profile_name: Optional[str] = Field(None, description="AWS profile name")
    endpoint_url: Optional[str] = Field(
        None, description="Custom endpoint URL for localstack or other services"
    )


class AWSSecretsManagerProvider:
    """AWS Secrets Manager implementation"""

    def __init__(self, config: AWSSecretsManagerConfig) -> None:
        """Initialize the AWS Secrets Manager provider with the given configuration"""
        self.config = config
        self._client = None

    def _get_client(self) -> SecretsManagerClient:
        """Lazy initialization of boto3 client

        Returns:
            Initialized boto3 Secrets Manager client

        Raises:
            RuntimeError: If client initialization fails
        """
        if self._client is None:
            try:
                # If endpoint_url is provided, use test credentials for LocalStack
                if self.config.endpoint_url:
                    session = boto3.Session(
                        aws_access_key_id="test",
                        aws_secret_access_key="test",
                        region_name=self.config.region_name,
                    )
                    self._client = session.client(
                        "secretsmanager",
                        endpoint_url=self.config.endpoint_url,
                        region_name=self.config.region_name,
                        config=Config(
                            signature_version="s3v4",
                            retries={"max_attempts": 0},
                        ),
                    )
                else:
                    # Use normal AWS credentials for production
                    session = boto3.Session(
                        region_name=self.config.region_name,
                        profile_name=self.config.profile_name,
                    )

                    if self.config.role_arn:
                        logger.debug(f"Assuming role: {self.config.role_arn}")
                        sts = session.client("sts")
                        assumed_role = sts.assume_role(
                            RoleArn=self.config.role_arn,
                            RoleSessionName="AlembicDumpSecretAccess",
                        )
                        credentials = assumed_role["Credentials"]
                        session = boto3.Session(
                            aws_access_key_id=credentials["AccessKeyId"],
                            aws_secret_access_key=credentials["SecretAccessKey"],
                            aws_session_token=credentials["SessionToken"],
                            region_name=self.config.region_name,
                        )

                    self._client = session.client("secretsmanager")

                logger.debug("Successfully initialized AWS Secrets Manager client")
            except Exception as e:
                logger.error(f"Failed to initialize AWS Secrets Manager client: {e}")
                raise RuntimeError(
                    f"Failed to initialize AWS Secrets Manager client: {e}"
                ) from e
        return self._client

    def get_secret_value(self, key_name: str) -> Optional[str]:
        """Get a specific secret value by key name"""
        try:
            response = self._get_client().get_secret_value(
                SecretId=self.config.secret_id
            )
            secret_string = response["SecretString"]
            import json

            try:
                # Try to parse as JSON
                secret_dict = json.loads(secret_string)
                return secret_dict.get(key_name)
            except json.JSONDecodeError:
                # If not JSON, return the whole string if key_name matches a convention
                if key_name == "password":
                    return secret_string
                return None
        except Exception as e:
            raise RuntimeError(f"Failed to get secret value: {e}") from e

    def get_all_secrets(self) -> dict[str, Optional[str]]:
        """Get all secrets as a dictionary"""
        try:
            response = self._get_client().get_secret_value(
                SecretId=self.config.secret_id
            )
            secret_string = response["SecretString"]
            import json

            try:
                # Try to parse as JSON
                return json.loads(secret_string)
            except json.JSONDecodeError:
                # If not JSON, return as single password
                return {"password": secret_string}
        except Exception as e:
            raise RuntimeError(f"Failed to get all secrets: {e}") from e


# --- HashiCorp Vault ---
class HashiCorpVaultConfig(BaseModel):
    """Configuration for HashiCorp Vault"""

    provider_type: str = "hashicorp_vault"
    vault_addr: str = Field(description="Vault server address")
    secret_path: str = Field(description="Path to the secret in Vault")
    vault_token: Optional[SecretStr] = Field(
        None, description="Vault token for authentication"
    )
    role_id: Optional[str] = Field(None, description="AppRole role ID")
    secret_id: Optional[SecretStr] = Field(None, description="AppRole secret ID")


class HashiCorpVaultProvider:
    """HashiCorp Vault implementation"""

    def __init__(self, config: HashiCorpVaultConfig) -> None:
        self.config = config
        self._client = None

    def _get_client(self) -> hvac.Client:
        """Lazy initialization of hvac client"""
        if self._client is None:
            client = hvac.Client(url=self.config.vault_addr)

            if self.config.vault_token:
                client.token = self.config.vault_token.get_secret_value()
            elif self.config.role_id and self.config.secret_id:
                client.auth.approle.login(
                    role_id=self.config.role_id,
                    secret_id=self.config.secret_id.get_secret_value(),
                )
            else:
                raise ValueError(
                    "Either vault_token or role_id/secret_id must be provided"
                )

            self._client = client
        return self._client

    def get_secret_value(self, key_name: str) -> Optional[str]:
        """Get a specific secret value by key name"""
        try:
            response = self._get_client().secrets.kv.v2.read_secret_version(
                path=self.config.secret_path
            )
            if response and "data" in response and "data" in response["data"]:
                return response["data"]["data"].get(key_name)
            return None
        except Exception as e:
            raise RuntimeError(f"Failed to get secret value: {e}") from e

    def get_all_secrets(self) -> dict[str, Optional[str]]:
        """Get all secrets as a dictionary"""
        try:
            response = self._get_client().secrets.kv.v2.read_secret_version(
                path=self.config.secret_path
            )
            if response and "data" in response and "data" in response["data"]:
                return response["data"]["data"]
            return {}
        except Exception as e:
            raise RuntimeError(f"Failed to get all secrets: {e}") from e


# --- Factory function ---
def create_secret_provider(
    config: Union[AWSSecretsManagerConfig, HashiCorpVaultConfig],
) -> SecretProvider:
    """Factory function to create appropriate secret provider"""
    if isinstance(config, AWSSecretsManagerConfig):
        return AWSSecretsManagerProvider(config)
    elif isinstance(config, HashiCorpVaultConfig):
        return HashiCorpVaultProvider(config)
    else:
        raise ValueError(f"Unsupported secret provider config type: {type(config)}")
