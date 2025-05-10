import time

import docker
import pytest

POSTGRES_IMAGE = "postgres:15"
POSTGRES_PASSWORD = "testpass"
POSTGRES_USER = "testuser"
POSTGRES_DB = "testdb"

@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()

@pytest.fixture(scope="session")
def postgres_containers(docker_client):
    containers = []
    for i in range(2):
        container = docker_client.containers.run(
            POSTGRES_IMAGE,
            name=f"pg_test_{i}",
            environment={
                "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
                "POSTGRES_USER": POSTGRES_USER,
                "POSTGRES_DB": POSTGRES_DB,
            },
            ports={"5432/tcp": None},  # 랜덤 포트 할당
            detach=True,
            remove=True,
        )
        containers.append(container)
    # 컨테이너가 준비될 때까지 대기
    time.sleep(5)
    yield containers
    for c in containers:
        c.stop()

@pytest.fixture(scope="session")
def pg_conn_infos(postgres_containers):
    infos = []
    for c in postgres_containers:
        c.reload()
        port = c.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
        infos.append({
            "host": "127.0.0.1",
            "port": int(port),
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "database": POSTGRES_DB,
        })
    return infos