import hashlib
import random
import re
import string
from typing import Any, Callable

from .config import MaskingConfig, MaskingRule


class MaskingManager:
    """데이터 마스킹 관리 클래스"""

    def __init__(self, config: MaskingConfig) -> None:
        self.config = config
        self._rules: dict[str, Callable[[Any], str]] = {
            'email': self._mask_email,
            'phone': self._mask_phone,
            'name': self._mask_name,
            'address': self._mask_address,
            'credit_card': self._mask_credit_card,
            'hash': self._hash_value,
        }

    def mask_value(self, value: Any, rule: MaskingRule) -> str:
        """값을 마스킹 처리"""
        if value is None:
            return ''
        
        # strategy 필드 사용
        if rule.strategy not in self._rules:
            raise ValueError(f"Unknown masking strategy: {rule.strategy}")
        
        return self._rules[rule.strategy](str(value))

    def _mask_email(self, email: str) -> str:
        """이메일 마스킹"""
        if not email or '@' not in email:
            return email
        
        username, domain = email.split('@')
        masked_username = username[0] + '*' * (len(username) - 1)
        return f"{masked_username}@{domain}"

    def _mask_phone(self, phone: str) -> str:
        """전화번호 마스킹"""
        digits = re.sub(r'\D', '', phone)
        if len(digits) < 10:
            return phone
        
        return f"{digits[:3]}-****-{digits[-4:]}"

    def _mask_name(self, name: str) -> str:
        """이름 마스킹"""
        if not name:
            return name
        return name[0] + '*' * (len(name) - 1)

    def _mask_address(self, address: str) -> str:
        """주소 마스킹"""
        if not address:
            return address
        parts = address.split()
        if len(parts) <= 1:
            return address
        return f"{parts[0]} {'*' * len(parts[1])} {' '.join(parts[2:])}"

    def _mask_credit_card(self, card: str) -> str:
        """신용카드 마스킹"""
        digits = re.sub(r'\D', '', card)
        if len(digits) < 13:
            return card
        return f"{digits[:4]}-****-****-{digits[-4:]}"

    def _hash_value(self, value: str) -> str:
        """값 해싱"""
        if not value:
            return value
        return hashlib.sha256(value.encode()).hexdigest()

    def _generate_random_string(self, length: int) -> str:
        """랜덤 문자열 생성"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def create_masking_manager(config: MaskingConfig) -> MaskingManager:
    """마스킹 매니저 생성 헬퍼 함수"""
    return MaskingManager(config)
