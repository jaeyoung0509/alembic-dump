from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from types import TracebackType
from typing import Any, TextIO
from uuid import UUID

from sqlalchemy import MetaData, Table, text
from sqlalchemy.engine import Engine
from typing_extensions import Self

from .config import MaskingConfig
from .exporters import BaseExporter, CsvExporter, JsonlExporter
from .masking import MaskingManager

_SAFE_QUERY_PREFIX = ("select", "with")
_BANNED_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "merge",
    "create",
    "replace",
    "grant",
    "revoke",
)


@dataclass
class ExportOptions:
    format: str
    output: str
    table: str | None = None
    query: str | None = None
    chunk_size: int = 100000
    encoding: str = "utf-8"
    delimiter: str = ","
    null_value: str = ""
    include_header: bool = True
    ensure_ascii: bool = False
    json_array: bool = False
    decimal_as: str = "string"
    bytes_as: str = "base64"
    unsafe_query: bool = False
    masking_table: str | None = None


class _OutputHandle:
    def __init__(self, output: str, encoding: str) -> None:
        self._output = output
        self._encoding = encoding
        self.stream: TextIO | None = None
        self._should_close = False

    def __enter__(self) -> Self:
        import sys

        if self._output == "-":
            self.stream = sys.stdout
            self._should_close = False
            return self
        self.stream = open(self._output, "w", encoding=self._encoding, newline="")
        self._should_close = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._should_close and self.stream:
            self.stream.close()


def validate_query_is_safe(query: str) -> None:
    cleaned = _strip_sql_comments(query).strip().lower()
    if not cleaned:
        raise ValueError("Query is empty.")
    if ";" in cleaned:
        raise ValueError("Multiple statements are not allowed.")
    if not cleaned.startswith(_SAFE_QUERY_PREFIX):
        raise ValueError("Only SELECT/CTE queries are allowed.")
    if re.search(r"\b(" + "|".join(_BANNED_KEYWORDS) + r")\b", cleaned):
        raise ValueError("Query contains a disallowed keyword.")


def export_data(
    engine: Engine,
    options: ExportOptions,
    masking: MaskingConfig | None = None,
) -> None:
    if bool(options.table) == bool(options.query):
        raise ValueError("Exactly one of table or query must be provided.")

    export_format = options.format.lower()
    if export_format not in {"csv", "jsonl", "json"}:
        raise ValueError(f"Unsupported format: {options.format}")

    if options.query and not options.unsafe_query:
        validate_query_is_safe(options.query)

    json_array = options.json_array or export_format == "json"

    mask_manager = MaskingManager(masking) if masking else None

    with _OutputHandle(options.output, options.encoding) as handle:
        exporter = _create_exporter(
            export_format,
            handle.stream,
            options,
            json_array=json_array,
        )
        if options.table:
            _export_table(
                engine,
                options.table,
                exporter,
                options,
                mask_manager,
                masking,
            )
        else:
            _export_query(
                engine,
                options.query or "",
                exporter,
                options,
                mask_manager,
                masking,
            )


def _create_exporter(
    format_name: str,
    stream: TextIO,
    options: ExportOptions,
    json_array: bool,
) -> BaseExporter:
    if format_name == "csv":
        return CsvExporter(
            stream,
            delimiter=options.delimiter,
            null_value=options.null_value,
            include_header=options.include_header,
        )
    return JsonlExporter(
        stream,
        ensure_ascii=options.ensure_ascii,
        json_array=json_array,
    )


def _export_table(
    engine: Engine,
    table_name: str,
    exporter: BaseExporter,
    options: ExportOptions,
    mask_manager: MaskingManager | None,
    masking: MaskingConfig | None,
) -> None:
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    columns = [col.name for col in table.columns]

    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(table.select())
        result = result.mappings()
        exporter.start(columns)
        while True:
            rows = result.fetchmany(options.chunk_size)
            if not rows:
                break
            for row in rows:
                row_dict = dict(row)
                if mask_manager and masking:
                    row_dict = _apply_masking(
                        row_dict, table_name, mask_manager, masking
                    )
                normalized = _normalize_row(row_dict, options)
                exporter.write_row(normalized)
        exporter.finish()


def _export_query(
    engine: Engine,
    query: str,
    exporter: BaseExporter,
    options: ExportOptions,
    mask_manager: MaskingManager | None,
    masking: MaskingConfig | None,
) -> None:
    mask_table = options.masking_table
    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(text(query))
        result = result.mappings()
        columns = list(result.keys())
        exporter.start(columns)
        while True:
            rows = result.fetchmany(options.chunk_size)
            if not rows:
                break
            for row in rows:
                row_dict = dict(row)
                if mask_manager and masking and mask_table:
                    row_dict = _apply_masking(
                        row_dict, mask_table, mask_manager, masking
                    )
                normalized = _normalize_row(row_dict, options)
                exporter.write_row(normalized)
        exporter.finish()


def _apply_masking(
    row: dict[str, Any],
    table_name: str,
    mask_manager: MaskingManager,
    masking: MaskingConfig,
) -> dict[str, Any]:
    rules = masking.rules.get(table_name, {}) if masking else {}
    if not rules:
        return row
    masked = dict(row)
    for column, rule in rules.items():
        if column in masked:
            masked[column] = mask_manager.mask_value(masked[column], rule)
    return masked


def _normalize_row(row: dict[str, Any], options: ExportOptions) -> dict[str, object]:
    return {key: _normalize_value(value, options) for key, value in row.items()}


def _normalize_value(value: object, options: ExportOptions) -> object:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, Decimal):
        if options.decimal_as == "float":
            return float(value)
        return str(value)
    if isinstance(value, bytes):
        if options.bytes_as == "hex":
            return value.hex()
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {k: _normalize_value(v, options) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_value(v, options) for v in value]
    return value


def _strip_sql_comments(query: str) -> str:
    query = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
    query = re.sub(r"--.*?$", " ", query, flags=re.MULTILINE)
    return query
