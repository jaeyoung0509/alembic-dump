from sqlalchemy import Column, ForeignKey, Integer, MetaData

from alembic_dump.db import Table
from alembic_dump.utils import detect_circular_dependencies


def test_no_circular_dependencies():
    metadata = MetaData()
    Table("table_a", metadata, Column("id", Integer, primary_key=True))
    Table(
        "table_b",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("a_id", Integer, ForeignKey("table_a.id")),
    )
    Table(
        "table_c",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("b_id", Integer, ForeignKey("table_b.id")),
    )

    assert detect_circular_dependencies(metadata) == []


def test_circular_dependencies():
    metadata = MetaData()
    Table(
        "table_a",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("b_id", Integer, ForeignKey("table_b.id")),
    )
    Table(
        "table_b",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("a_id", Integer, ForeignKey("table_a.id")),
    )

    cycles = detect_circular_dependencies(metadata)
    assert len(cycles) == 1
    assert cycles[0] == {"table_a", "table_b"}
