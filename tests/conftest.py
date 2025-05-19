import logging
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Generator
from typing import Any, Optional, Union

import boto3
import hvac
import psycopg2
import pytest
from docker import DockerClient, errors, from_env
from docker.models.containers import Container

POSTGRES_IMAGE = "postgres:15"
POSTGRES_PASSWORD = "testpass"
POSTGRES_USER = "testuser"
POSTGRES_DB = "testdb"

DOCKER_NETWORK_NAME = "integration_test_network"

# Secret Management Test Constants
LOCALSTACK_IMAGE = "localstack/localstack:latest"
VAULT_IMAGE = "hashicorp/vault:latest"
VAULT_ROOT_TOKEN = "root"

logger = logging.getLogger(__name__)

# pylint: disable=line-too-long


@pytest.fixture(scope="session")
def docker_client_session() -> DockerClient:
    try:
        client = from_env()
        client.ping()
        logger.info("Successfully connected to Docker daemon.")
        return client
    except errors.DockerException as exc:
        logger.error("Failed to connect to Docker daemon: %s", exc)
        pytest.skip(f"Docker not available or not running, {exc}")


@pytest.fixture(scope="session")
def test_docker_network(
    docker_client_session: DockerClient,
) -> Generator[Optional[str], None, None]:
    try:
        network = docker_client_session.networks.get(DOCKER_NETWORK_NAME)
        logging.info(f"Using existing network: {DOCKER_NETWORK_NAME}")
    except errors.NotFound:
        logging.info(f"Creating new network: {DOCKER_NETWORK_NAME}")
        network = docker_client_session.networks.create(
            DOCKER_NETWORK_NAME, driver="bridge", check_duplicate=True
        )
    yield network.name


def _create_postgres_container(
    docker_client: DockerClient,
    network_name: str,
    container_name_prefix: str,
    db_name: str = POSTGRES_DB,
    user: str = POSTGRES_USER,
    password: str = POSTGRES_PASSWORD,
    image: str = POSTGRES_IMAGE,
) -> dict[str, Union[str, int, Container]]:
    container_name = f"{container_name_prefix}_{int(time.time())}"
    try:
        logger.info(f"Creating container: {container_name}")
        container = docker_client.containers.run(
            image,
            name=container_name,
            environment={
                "POSTGRES_PASSWORD": password,
                "POSTGRES_USER": user,
                "POSTGRES_DB": db_name,
            },
            ports={"5432/tcp": None},  # 랜덤 포트 할당
            detach=True,
            remove=True,
            network=network_name,
        )

        for i in range(15):
            time.sleep(2)
            container.reload()
            if container.status != "running":
                logs = container.logs().decode("utf-8")
                logger.error(f"Container {container.name} is not running: {logs}")
                raise RuntimeError(f"Container {container.name} failed to start.")
            host_port_info = container.attrs["NetworkSettings"]["Ports"].get("5432/tcp")
            if not host_port_info or not host_port_info[0].get("HostPort"):
                logger.debug(f"Container {container.name} port info not available yet.")
                continue
            host_port = host_port_info[0]["HostPort"]
            try:
                conn = psycopg2.connect(
                    host="localhost",  # 호스트 머신에서 접속 시
                    port=host_port,
                    user=user,
                    password=password,
                    dbname=db_name,
                    connect_timeout=2,  # 짧은 타임아웃
                )
                conn.close()
                logger.info(
                    f"PostgreSQL container '{container_name}' is ready on host port {host_port} (internal 5432)."
                )
                return {
                    "name": container_name,
                    "host_for_docker_network": container_name,
                    "port_internal": 5432,
                    "host_for_host_machine": "localhost",
                    "port_on_host": int(host_port),
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "container_obj": container,
                    "sqlalchemy_url_in_docker_net": f"postgresql://{user}:{password}@{container_name}:5432/{db_name}",
                    "sqlalchemy_url_on_host": f"postgresql://{user}:{password}@localhost:{host_port}/{db_name}",
                }
            except (psycopg2.OperationalError, TypeError, KeyError) as e:
                logger.debug(
                    f"Waiting for PostgreSQL container '{container_name}' (attempt {i + 1}/15)... Error: {e}"
                )

        # 루프 종료 후에도 준비 안되면 오류 발생
        logs = container.logs().decode("utf-8", errors="ignore")
        logger.error(
            f"PostgreSQL container '{container_name}' did not become ready in time. Logs:\n{logs}"
        )
        raise TimeoutError(
            f"PostgreSQL container '{container_name}' did not become ready in time."
        )

    except Exception as e:  # 컨테이너 시작 실패 등
        logger.error(
            f"Failed to start/setup PostgreSQL container '{container_name}': {e}",
            exc_info=True,
        )
        # 생성 시도한 컨테이너가 있다면 강제 삭제 시도 (오류 무시)
        if "container" in locals() and container:
            try:
                container.remove(force=True)
                logger.info(
                    f"Force removed container '{container_name}' after startup failure."
                )
            except Exception:
                pass
        raise  # 원래 예외를 다시 발생시켜 테스트가 실패하도록 함


@pytest.fixture(scope="session")
def source_pg_container(
    docker_client_session: DockerClient, test_docker_network: str
) -> dict[str, Union[str, int, Container]]:
    """SSH 터널의 원격 목적지가 될 소스 PostgreSQL 컨테이너 정보"""
    return _create_postgres_container(
        docker_client_session, test_docker_network, "pg_source_ssh_test"
    )


@pytest.fixture(scope="session")
def target_pg_container(
    docker_client_session: DockerClient, test_docker_network: str
) -> dict[str, Union[str, int, Container]]:
    """일반적인 타겟 PostgreSQL 컨테이너 정보"""
    return _create_postgres_container(
        docker_client_session, test_docker_network, "pg_target_ssh_test"
    )


@pytest.fixture(scope="session")
def ssh_test_key_pair() -> Generator[dict[str, str], None, None]:
    temp_dir = tempfile.mkdtemp(prefix="ssh_test_")
    private_key_path = f"{temp_dir}/id_rsa"
    public_key_path = f"{temp_dir}/id_rsa.pub"
    ssh_user = "testuser"
    try:
        subprocess.run(
            [
                "ssh-keygen",
                "-t",
                "rsa",
                "-b",
                "2048",
                "-f",
                private_key_path,
                "-N",
                "",
                "-C",
                ssh_user,
            ],
            check=True,
        )
        with open(private_key_path) as f:
            private_key = f.read().strip()
        with open(public_key_path) as f:
            public_key = f.read().strip()
        logger.info(f"SSH key pair generated at {temp_dir}")
        yield {
            "private_key": private_key,
            "public_key": public_key,
            "private_key_path": private_key_path,
            "public_key_path": public_key_path,
            "user_name": ssh_user,
        }
    finally:
        logger.debug(f"Cleaning up SSH key pair at {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def ssh_server_container(
    docker_client_session: DockerClient,
    ssh_test_key_pair: dict[str, str],
    test_docker_network: str,
) -> Generator[dict[str, Any], None, None]:
    ssh_server_image = "linuxserver/openssh-server:latest"
    container_name = "alembic_dump_test_ssh_bastion"
    ssh_user = ssh_test_key_pair["user_name"]
    public_key = ssh_test_key_pair["public_key"]
    private_key_path = ssh_test_key_pair["private_key_path"]

    try:
        existing = docker_client_session.containers.get(container_name)
        existing.remove(force=True)
    except errors.NotFound:
        pass

    logger.info(
        f"Starting SSH server container '{container_name}' with user '{ssh_user}'..."
    )

    try:
        container = docker_client_session.containers.run(
            ssh_server_image,
            name=container_name,
            hostname=container_name,
            environment={
                "PUID": "1000",
                "PGID": "1000",
                "USER_NAME": ssh_user,
                "PUBLIC_KEY": public_key,
                "SUDO_ACCESS": "false",
                "PASSWORD_ACCESS": "false",
                "DOCKER_MODS": "linuxserver/mods:openssh-server-ssh-tunnel",
                "SHELL_NOLOGIN": "false",
            },
            ports={"2222/tcp": None},
            network=test_docker_network,
            detach=True,
            remove=True,
        )

        # 컨테이너 시작 후 포트 정보 확인
        container.reload()
        host_port = None
        for i in range(30):  # 최대 30초 대기
            container.reload()
            port_info = container.attrs["NetworkSettings"]["Ports"].get("2222/tcp")
            if port_info and port_info[0].get("HostPort"):
                host_port = int(port_info[0]["HostPort"])
                break
            time.sleep(1)

        if not host_port:
            raise RuntimeError("Failed to get SSH server port mapping")

        # SSH 서버 준비 상태 확인
        for i in range(30):
            try:
                import paramiko

                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(
                    "localhost",
                    port=host_port,
                    username=ssh_user,
                    key_filename=private_key_path,
                    timeout=5,
                )
                ssh.close()
                logger.info(f"SSH server is ready on port {host_port}")
                break
            except Exception as e:
                logger.debug(f"Waiting for SSH server (attempt {i + 1}): {e}")
                time.sleep(1)
        else:
            raise RuntimeError("SSH server did not become ready in time")

        yield {
            "name": container_name,
            "host_for_docker_network": container_name,
            "port_internal": 2222,
            "host_for_host_machine": "localhost",
            "port_on_host": host_port,
            "user": ssh_user,
            "private_key_path": private_key_path,
            "container_obj": container,
        }
    except Exception as exc:
        logger.error(
            f"Failed to start SSH server container '{container_name}': {exc}",
            exc_info=True,
        )
        if container:
            try:
                container.remove(force=True)
                logger.info(
                    f"Force removed container '{container_name}' after startup failure."
                )
            except Exception:
                pass
            pytest.fail(
                f"Failed to start SSH server container '{container_name}': {exc}"
            )


@pytest.fixture(scope="session")
def alembic_test_env_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "alembic_test_env"))


@pytest.fixture(scope="session")
def localstack_container(
    docker_client_session: DockerClient, test_docker_network: str
) -> dict[str, Union[str, int, Container]]:
    """LocalStack container for AWS Secrets Manager emulation"""
    container_name = f"localstack_secrets_test_{int(time.time())}"
    try:
        logger.info(f"Creating LocalStack container: {container_name}")
        container = docker_client_session.containers.run(
            LOCALSTACK_IMAGE,
            name=container_name,
            environment={
                "SERVICES": "secretsmanager",
                "DEBUG": "1",
                "DATA_DIR": "/tmp/localstack/data",
                "DOCKER_HOST": "unix:///var/run/docker.sock",
            },
            ports={"4566/tcp": None},  # Random port allocation
            volumes=[
                f"{os.path.abspath('./volume')}:/var/lib/localstack",
                "/var/run/docker.sock:/var/run/docker.sock",
            ],
            detach=True,
            remove=True,
            network=test_docker_network,
        )

        # Wait for LocalStack to be ready
        for i in range(15):
            time.sleep(2)
            container.reload()
            if container.status != "running":
                logs = container.logs().decode("utf-8")
                logger.error(f"Container {container.name} is not running: {logs}")
                raise RuntimeError(f"Container {container.name} failed to start.")

            host_port_info = container.attrs["NetworkSettings"]["Ports"].get("4566/tcp")
            if not host_port_info or not host_port_info[0].get("HostPort"):
                logger.debug(f"Container {container.name} port info not available yet.")
                continue

            host_port = host_port_info[0]["HostPort"]
            try:
                # Test LocalStack connection
                session = boto3.session.Session()
                client = session.client(
                    "secretsmanager",
                    endpoint_url=f"http://localhost:{host_port}",
                    region_name="us-east-1",
                    aws_access_key_id="test",
                    aws_secret_access_key="test",
                )
                client.list_secrets()  # Test API call
                logger.info(
                    f"LocalStack container '{container_name}' is ready on host port {host_port}."
                )
                return {
                    "name": container_name,
                    "host_for_docker_network": container_name,
                    "port_internal": 4566,
                    "host_for_host_machine": "localhost",
                    "port_on_host": int(host_port),
                    "container_obj": container,
                    "endpoint_url": f"http://localhost:{host_port}",
                }
            except Exception as e:
                logger.debug(
                    f"Waiting for LocalStack container '{container_name}' (attempt {i + 1}/15)... Error: {e}"
                )

        logs = container.logs().decode("utf-8", errors="ignore")
        logger.error(
            f"LocalStack container '{container_name}' did not become ready in time. Logs:\n{logs}"
        )
        raise TimeoutError(
            f"LocalStack container '{container_name}' did not become ready in time."
        )

    except Exception as e:
        logger.error(
            f"Failed to start/setup LocalStack container '{container_name}': {e}",
            exc_info=True,
        )
        if "container" in locals() and container:
            try:
                container.remove(force=True)
                logger.info(
                    f"Force removed container '{container_name}' after startup failure."
                )
            except Exception:
                pass
        raise


@pytest.fixture(scope="session")
def vault_container(
    docker_client_session: DockerClient, test_docker_network: str
) -> dict[str, Union[str, int, Container]]:
    """HashiCorp Vault container for secret management testing"""
    container_name = f"vault_test_{int(time.time())}"
    try:
        logger.info(f"Creating Vault container: {container_name}")
        container = docker_client_session.containers.run(
            VAULT_IMAGE,
            name=container_name,
            environment={
                "VAULT_DEV_ROOT_TOKEN_ID": VAULT_ROOT_TOKEN,
                "VAULT_DEV_LISTEN_ADDRESS": "0.0.0.0:8200",
            },
            ports={"8200/tcp": None},  # Random port allocation
            cap_add=["IPC_LOCK"],
            command="server -dev -dev-root-token-id=root",
            detach=True,
            remove=True,
            network=test_docker_network,
        )

        # Wait for Vault to be ready
        for i in range(15):
            time.sleep(2)
            container.reload()
            if container.status != "running":
                logs = container.logs().decode("utf-8")
                logger.error(f"Container {container.name} is not running: {logs}")
                raise RuntimeError(f"Container {container.name} failed to start.")

            host_port_info = container.attrs["NetworkSettings"]["Ports"].get("8200/tcp")
            if not host_port_info or not host_port_info[0].get("HostPort"):
                logger.debug(f"Container {container.name} port info not available yet.")
                continue

            host_port = host_port_info[0]["HostPort"]
            try:
                # Test Vault connection
                client = hvac.Client(
                    url=f"http://localhost:{host_port}",
                    token=VAULT_ROOT_TOKEN,
                )
                if client.is_authenticated():
                    logger.info(
                        f"Vault container '{container_name}' is ready on host port {host_port}."
                    )
                    return {
                        "name": container_name,
                        "host_for_docker_network": container_name,
                        "port_internal": 8200,
                        "host_for_host_machine": "localhost",
                        "port_on_host": int(host_port),
                        "container_obj": container,
                        "token": VAULT_ROOT_TOKEN,
                        "url": f"http://localhost:{host_port}",
                    }
            except Exception as e:
                logger.debug(
                    f"Waiting for Vault container '{container_name}' (attempt {i + 1}/15)... Error: {e}"
                )

        logs = container.logs().decode("utf-8", errors="ignore")
        logger.error(
            f"Vault container '{container_name}' did not become ready in time. Logs:\n{logs}"
        )
        raise TimeoutError(
            f"Vault container '{container_name}' did not become ready in time."
        )

    except Exception as e:
        logger.error(
            f"Failed to start/setup Vault container '{container_name}': {e}",
            exc_info=True,
        )
        if "container" in locals() and container:
            try:
                container.remove(force=True)
                logger.info(
                    f"Force removed container '{container_name}' after startup failure."
                )
            except Exception:
                pass
        raise
