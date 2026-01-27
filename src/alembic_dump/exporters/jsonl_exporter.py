from __future__ import annotations

import json
from typing import TextIO

from .base import BaseExporter


class JsonlExporter(BaseExporter):
    def __init__(
        self,
        stream: TextIO,
        ensure_ascii: bool = False,
        json_array: bool = False,
    ) -> None:
        super().__init__(stream)
        self._ensure_ascii = ensure_ascii
        self._json_array = json_array
        self._first_row = True

    def start(self, columns: list[str]) -> None:
        super().start(columns)
        if self._json_array:
            self._stream.write("[")

    def write_row(self, row: dict) -> None:
        if self._json_array:
            if not self._first_row:
                self._stream.write(",")
            self._stream.write(json.dumps(row, ensure_ascii=self._ensure_ascii))
            self._first_row = False
            return
        self._stream.write(json.dumps(row, ensure_ascii=self._ensure_ascii) + "\n")

    def finish(self) -> None:
        if self._json_array:
            self._stream.write("]\n")
