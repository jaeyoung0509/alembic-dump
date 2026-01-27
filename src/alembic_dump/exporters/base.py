from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TextIO


class BaseExporter(ABC):
    def __init__(self, stream: TextIO) -> None:
        self._stream = stream
        self._columns: list[str] = []

    def start(self, columns: Sequence[str]) -> None:
        self._columns = list(columns)

    @abstractmethod
    def write_row(self, row: dict) -> None:
        raise NotImplementedError

    def finish(self) -> None:
        return None
