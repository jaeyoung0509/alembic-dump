# tests/test_workflow.py
import logging
import os
import subprocess
from typing import Any

import psycopg2  # 데이터 직접 삽입/검증용
import pytest

from alembic_dump.config import AppSettings, DBConfig  # 실제 설정 객체 사용
from alembic_dump.core import dump_and_load  # 테스트 대상 함수

logger = logging.getLogger(__name__)

# 테스트용 Alembic 환경 디렉토리 경로 (conftest.py의 픽스처를 직접 사용하는 것이 더 좋음)
# 이 변수 대신 alembic_test_env_dir 픽스처를 직접 주입받아 사용하도록 변경 권장
ALEMBIC_DIR_WORKFLOW = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "alembic_test_env")
)


# 테스트 DB에 Alembic 마이그레이션 적용하는 헬퍼 함수
def _apply_migrations_workflow(
    db_url: str, alembic_env_path: str, revision: str = "head"
):
    """주어진 DB URL에 Alembic 마이그레이션을 적용합니다."""
    logger.info(
        f"Applying Alembic migrations to {db_url.split('@')[-1]} up to revision '{revision}' using ini from '{alembic_env_path}'"
    )
    try:
        subprocess.run(
            [
                "alembic",
                "-c",
                os.path.join(
                    alembic_env_path, "alembic.ini"
                ),  # alembic.ini 경로 명확히
                "-x",
                f"db_url={db_url}",  # db_url은 이미 완전한 형태
                "upgrade",
                revision,
            ],
            check=True,
            capture_output=True,  # 오류 발생 시 출력 확인용
            text=True,
            encoding="utf-8",
        )
        logger.info(
            f"Successfully applied Alembic migrations to {db_url.split('@')[-1]}."
        )
    except subprocess.CalledProcessError as e:
        logger.error(
            f"Failed to apply Alembic migrations to {db_url.split('@')[-1]}. Revision: {revision}"
        )
        logger.error(f"Alembic command: {' '.join(e.cmd)}")
        logger.error(f"Return code: {e.returncode}")
        if e.stdout:
            logger.error(f"Alembic stdout:\n{e.stdout}")
        if e.stderr:
            logger.error(f"Alembic stderr:\n{e.stderr}")
        raise


@pytest.mark.integration  # 기존 마커 유지 또는 변경
def test_full_dump_and_load(
    source_pg_container: dict[str, Any],  # 새로운 픽스처 사용
    target_pg_container: dict[str, Any],  # 새로운 픽스처 사용
    alembic_test_env_dir: str,  # conftest.py의 픽스처 사용
) -> None:
    """
    기존 full dump and load 테스트를 새로운 Docker 픽스처를 사용하여 실행합니다.
    이 테스트는 SSH 터널을 사용하지 않는 직접 연결 시나리오입니다.
    """
    logger.info("Starting test_full_dump_and_load (direct connection)...")

    # 1. 각 DB에 Alembic 스키마 적용
    # 테스트 코드 실행 환경(호스트 머신)에서 각 PG 컨테이너의 노출된 포트로 접속
    _apply_migrations_workflow(
        source_pg_container["sqlalchemy_url_on_host"], alembic_test_env_dir
    )
    _apply_migrations_workflow(
        target_pg_container["sqlalchemy_url_on_host"], alembic_test_env_dir
    )
    logger.info("Initial Alembic migrations applied to both test databases.")

    # 2. 테스트 데이터 소스 DB에 삽입
    # 호스트 머신에서 소스 PG 컨테이너의 노출된 포트로 접속
    with psycopg2.connect(
        host=source_pg_container["host_for_host_machine"],
        port=source_pg_container["port_on_host"],
        user=source_pg_container["user"],
        password=source_pg_container["password"],
        dbname=source_pg_container["database"],
    ) as conn:
        with conn.cursor() as cur:
            # users 테이블이 Alembic 마이그레이션에 의해 생성되었다고 가정
            cur.execute(
                "DELETE FROM users WHERE id = 1;"
            )  # 테스트 반복 실행 시 기존 데이터 삭제
            cur.execute(
                "INSERT INTO users (id, name, email) VALUES (1, '홍길동 직통', 'hong_direct@test.com')"
            )
            conn.commit()
    logger.info("Test data inserted into source DB for direct connection test.")

    source_db_app_config = DBConfig(
        driver="postgresql",
        host=source_pg_container["host_for_host_machine"],  # 호스트에서 접근
        port=source_pg_container["port_on_host"],
        username=source_pg_container["user"],
        password=source_pg_container["password"],
        database=source_pg_container["database"],
    )
    target_db_app_config = DBConfig(
        driver="postgresql",
        host=target_pg_container["host_for_host_machine"],  # 호스트에서 접근
        port=target_pg_container["port_on_host"],
        username=target_pg_container["user"],
        password=target_pg_container["password"],
        database=target_pg_container["database"],
    )

    app_settings = AppSettings(
        source_db=source_db_app_config,
        target_db=target_db_app_config,
        chunk_size=100,
        masking=None,
        tables_to_exclude=["alembic_version"],
        source_ssh_tunnel=None,
        target_ssh_tunnel=None,
    )
    logger.info("AppSettings configured for direct dump_and_load.")
    logger.debug(
        f"AppSettings for direct test: {app_settings.model_dump_json(indent=2)}"
    )

    # 4. dump_and_load 실행
    dump_and_load(
        app_settings, alembic_test_env_dir
    )  # 수정된 Alembic 디렉토리 픽스처 사용
    logger.info("dump_and_load (direct connection) executed.")

    with psycopg2.connect(
        host=target_pg_container["host_for_host_machine"],
        port=target_pg_container["port_on_host"],
        user=target_pg_container["user"],
        password=target_pg_container["password"],
        dbname=target_pg_container["database"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, email FROM users WHERE id=1")
            row = cur.fetchone()
            assert row is not None, "Data not found in target DB (direct connection)"
            assert row[0] == "홍길동 직통"
            assert row[1] == "hong_direct@test.com"
    logger.info("Data successfully verified in target DB for direct connection test.")
    logger.info("test_full_dump_and_load (direct connection) finished successfully.")
