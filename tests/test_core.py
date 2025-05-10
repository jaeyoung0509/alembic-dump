from sqlalchemy import Column, ForeignKey, Integer, MetaData, Table

from src.alembic_dump.utils import (
    apply_masking,
    chunk_iterable,
    detect_circular_dependencies,
    get_sorted_tables,
    mask_value,
)


def test_get_sorted_tables_and_circular():
    metadata = MetaData()
    t1 = Table('parent', metadata, Column('id', Integer, primary_key=True))
    t2 = Table('child', metadata, Column('id', Integer, primary_key=True), Column('parent_id', Integer, ForeignKey('parent.id')))
    sorted_tables = get_sorted_tables(metadata)
    assert sorted_tables[0].name == 'parent'
    assert sorted_tables[1].name == 'child'
    cycles = detect_circular_dependencies(metadata)
    assert cycles == []

def test_chunk_iterable():
    data = list(range(7))
    chunks = list(chunk_iterable(data, 3))
    assert chunks == [[0,1,2],[3,4,5],[6]]

def test_mask_value_hash():
    rule = {"strategy": "hash", "hash_salt": "abc"}
    v1 = mask_value("hello", rule)
    v2 = mask_value("hello", rule)
    assert v1 == v2
    assert v1 != "hello"

def test_mask_value_partial():
    rule = {"strategy": "partial", "partial_keep_chars": 2}
    assert mask_value("abcdef", rule) == "****ef"

def test_mask_value_null():
    rule = {"strategy": "null"}
    assert mask_value("something", rule) is None

def test_apply_masking():
    row = {"name": "홍길동", "email": "hong@test.com"}
    rules = {"users": {"email": {"strategy": "partial", "partial_keep_chars": 3}}}
    masked = apply_masking(row, "users", rules)
    assert masked["email"].endswith("com")
    assert masked["name"] == "홍길동"