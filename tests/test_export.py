from __future__ import annotations

import csv
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
)
from sqlalchemy.engine import Engine

from alembic_dump.config import MaskingConfig, MaskingRule
from alembic_dump.export import ExportOptions, export_data, validate_query_is_safe


def _setup_sqlite_db(tmp_path: Path) -> Engine:
    db_path = tmp_path / "export_test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    metadata = MetaData()
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("email", String(120)),
        Column("created_at", DateTime),
        Column("amount", Numeric(10, 2)),
        Column("payload", LargeBinary),
    )
    metadata.create_all(engine)
    rows = [
        {
            "id": 1,
            "name": "홍길동",
            "email": "hong@test.com",
            "created_at": datetime(2026, 1, 1, 12, 0, 0),
            "amount": Decimal("12.34"),
            "payload": b"\x00\xff",
        },
        {
            "id": 2,
            "name": "김철수",
            "email": "kim@test.com",
            "created_at": datetime(2026, 1, 2, 8, 30, 0),
            "amount": Decimal("99.99"),
            "payload": b"\x01\x02",
        },
        {
            "id": 3,
            "name": "이영희",
            "email": "lee@test.com",
            "created_at": datetime(2026, 1, 3, 9, 15, 0),
            "amount": Decimal("0.00"),
            "payload": b"\x10\x11",
        },
    ]
    with engine.begin() as conn:
        conn.execute(users.insert(), rows)
    return engine


def test_export_jsonl_applies_masking_and_serialization(tmp_path: Path) -> None:
    engine = _setup_sqlite_db(tmp_path)
    output_path = tmp_path / "users.jsonl"

    masking = MaskingConfig(rules={"users": {"email": MaskingRule(strategy="hash")}})
    options = ExportOptions(
        format="jsonl",
        output=str(output_path),
        table="users",
        chunk_size=2,
        ensure_ascii=False,
        decimal_as="string",
        bytes_as="base64",
    )

    export_data(engine, options, masking=masking)

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3

    first = json.loads(lines[0])
    assert first["email"] != "hong@test.com"
    assert len(first["email"]) == 64
    assert first["created_at"] == "2026-01-01T12:00:00"
    assert first["amount"] == "12.34"
    assert first["payload"] == "AP8="


def test_export_csv_with_header_and_null_value(tmp_path: Path) -> None:
    db_path = tmp_path / "csv_test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    metadata = MetaData()
    items = Table(
        "items",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("note", String(50)),
    )
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            items.insert(),
            [
                {"id": 1, "name": "alpha", "note": None},
                {"id": 2, "name": "beta", "note": "ok"},
            ],
        )

    output_path = tmp_path / "items.csv"
    options = ExportOptions(
        format="csv",
        output=str(output_path),
        table="items",
        chunk_size=1,
        delimiter="|",
        null_value="<NULL>",
        include_header=True,
    )

    export_data(engine, options)

    with output_path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="|")
        rows = list(reader)

    assert rows[0] == ["id", "name", "note"]
    assert rows[1] == ["1", "alpha", "<NULL>"]
    assert rows[2] == ["2", "beta", "ok"]


def test_export_json_array_format(tmp_path: Path) -> None:
    engine = _setup_sqlite_db(tmp_path)
    output_path = tmp_path / "users.json"

    options = ExportOptions(
        format="json",
        output=str(output_path),
        table="users",
        chunk_size=2,
    )

    export_data(engine, options)

    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert len(data) == 3
    assert data[0]["name"] == "홍길동"


def test_validate_query_is_safe() -> None:
    validate_query_is_safe("SELECT * FROM users")
    validate_query_is_safe("WITH cte AS (SELECT 1 AS id) SELECT * FROM cte")

    with pytest.raises(ValueError):
        validate_query_is_safe("DELETE FROM users")

    with pytest.raises(ValueError):
        validate_query_is_safe("SELECT * FROM users; DROP TABLE users")
