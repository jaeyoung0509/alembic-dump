import os
import subprocess

import psycopg2
import pytest

from src.alembic_dump.config import AppSettings, DBConfig
from src.alembic_dump.core import dump_and_load

ALEMBIC_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "alembic_test_env")
)


def run_alembic_upgrade(db_url, revision="head"):
    subprocess.run(
        [
            "alembic",
            "-c",
            f"{ALEMBIC_DIR}/alembic.ini",
            "-x",
            f"db_url={db_url}",
            "upgrade",
            revision,
        ],
        check=True,
    )


@pytest.mark.integration
def test_full_dump_and_load(pg_conn_infos):
    # 1. Apply latest schema to both databases using Alembic
    for info in pg_conn_infos:
        db_url = f"postgresql://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"
        run_alembic_upgrade(db_url)

    # 2. Insert test data into source database
    src_info = pg_conn_infos[0]
    conn = psycopg2.connect(
        host=src_info["host"],
        port=src_info["port"],
        user=src_info["user"],
        password=src_info["password"],
        dbname=src_info["database"],
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (id, name, email) VALUES (1, '홍길동', 'hong@test.com')"
            )
    conn.close()

    # 3. Prepare AppSettings configuration
    source_db = DBConfig(
        driver="postgresql",
        host=src_info["host"],
        port=src_info["port"],
        username=src_info["user"],
        password=src_info["password"],
        database=src_info["database"],
    )
    tgt_info = pg_conn_infos[1]
    target_db = DBConfig(
        driver="postgresql",
        host=tgt_info["host"],
        port=tgt_info["port"],
        username=tgt_info["user"],
        password=tgt_info["password"],
        database=tgt_info["database"],
    )
    settings = AppSettings(
        source_db=source_db,
        target_db=target_db,
        chunk_size=100,
        masking=None,
        tables_to_exclude=["alembic_version"],
    )

    # 4. Execute dump_and_load operation
    dump_and_load(settings, ALEMBIC_DIR)

    # 5. Verify data was correctly copied to target database
    conn = psycopg2.connect(
        host=tgt_info["host"],
        port=tgt_info["port"],
        user=tgt_info["user"],
        password=tgt_info["password"],
        dbname=tgt_info["database"],
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, email FROM users WHERE id=1")
            row = cur.fetchone()
            assert row is not None, "No data found in the target database"
            assert row[0] == "홍길동"
            assert row[1] == "hong@test.com"
    conn.close()
