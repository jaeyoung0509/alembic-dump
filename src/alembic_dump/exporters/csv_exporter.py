from __future__ import annotations

import csv
import json
from typing import Any, TextIO

from .base import BaseExporter


class CsvExporter(BaseExporter):
    def __init__(
        self,
        stream: TextIO,
        delimiter: str = ",",
        null_value: str = "",
        include_header: bool = True,
    ) -> None:
        super().__init__(stream)
        self._writer = csv.writer(stream, delimiter=delimiter)
        self._null_value = null_value
        self._include_header = include_header

    def start(self, columns: list[str]) -> None:
        super().start(columns)
        if self._include_header:
            self._writer.writerow(self._columns)

    def write_row(self, row: dict[str, Any]) -> None:
        values = []
        for column in self._columns:
            value = row.get(column)
            if value is None:
                values.append(self._null_value)
                continue
            if isinstance(value, (dict, list)):
                values.append(json.dumps(value, ensure_ascii=False))
                continue
            values.append(str(value))
        self._writer.writerow(values)
