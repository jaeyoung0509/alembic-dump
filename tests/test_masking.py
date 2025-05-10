from src.alembic_dump.config import MaskingConfig, MaskingRule
from src.alembic_dump.masking import MaskingManager


def test_masking_manager_email():
    rule = MaskingRule(strategy="email")
    config = MaskingConfig(rules={"users": {"email": rule}})
    mm = MaskingManager(config)
    masked = mm.mask_value("hong@test.com", rule)
    assert masked.startswith("h*")

def test_masking_manager_hash():
    rule = MaskingRule(strategy="hash")
    config = MaskingConfig(rules={"users": {"id": rule}})
    mm = MaskingManager(config)
    v1 = mm.mask_value("123", rule)
    v2 = mm.mask_value("123", rule)
    assert v1 == v2
    assert v1 != "123"

def test_masking_manager_name():
    rule = MaskingRule(strategy="name")
    config = MaskingConfig(rules={"users": {"name": rule}})
    mm = MaskingManager(config)
    masked = mm.mask_value("홍길동", rule)
    assert masked.startswith("홍")
    assert "*" in masked