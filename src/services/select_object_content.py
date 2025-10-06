"""
SelectObjectContent

https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html

This service enables applications to filter the contents of an Amazon S3 object
using simple SQL expressions.
"""

import csv
import io
import json
import re
import struct
import zlib
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from fastapi import Response
from fastapi.responses import StreamingResponse

from .storage import get_blob_content

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


@dataclass
class OutputConfig:
    format: str
    field_delimiter: str = ","
    record_delimiter: str = "\n"
    json_type: str = "LINES"


SQL_PATTERN = re.compile(
    r"SELECT\s+(?P<select>.+?)\s+FROM\s+S3OBJECT(?:\s+WHERE\s+(?P<where>.+))?$",
    re.IGNORECASE | re.DOTALL,
)
WHERE_PATTERN = re.compile(
    r'^(?P<column>[A-Za-z0-9_".]+)\s*(?P<op>=|!=|<>|>=|<=|>|<)\s*(?P<value>.+)$',
    re.IGNORECASE,
)


def _ns(tag: str) -> str:
    return f"{{{S3_NS}}}{tag}"


def _decode_text(element: Optional[ET.Element]) -> Optional[str]:
    if element is None or element.text is None:
        return None
    return element.text.strip()


def _parse_sql(sql: str) -> Tuple[List[str], Optional[str]]:
    sql = sql.strip().rstrip(";")
    match = SQL_PATTERN.match(sql)
    if not match:
        raise ValueError(
            "Invalid SQL expression. Supported: SELECT <columns> FROM S3Object [WHERE <condition>]."
        )

    select_part = match.group("select").strip()
    where_part = match.group("where")

    if select_part == "*":
        columns = ["*"]
    else:
        columns = [
            segment.strip() for segment in select_part.split(",") if segment.strip()
        ]
        if not columns:
            raise ValueError("SELECT clause must list at least one column or *.")

    return columns, where_part.strip() if where_part else None


def _clean_identifier(identifier: str) -> str:
    identifier = identifier.strip()
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1]
    if identifier.startswith("`") and identifier.endswith("`"):
        return identifier[1:-1]
    return identifier


def _parse_where_clause(where_clause: str) -> Tuple[str, str, str]:
    match = WHERE_PATTERN.match(where_clause.strip())
    if not match:
        raise ValueError(
            "Unsupported WHERE clause. Supported: <column> <operator> <value>."
        )

    column = _clean_identifier(match.group("column"))
    op = match.group("op")
    if op == "<>":
        op = "!="
    value = match.group("value").strip()
    return column, op, value


def _parse_literal(value: str):
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _coerce_scalar(column: pa.ChunkedArray, value):
    target_type = column.type
    try:
        if pa.types.is_integer(target_type):
            return pa.scalar(int(value), type=target_type)
        if pa.types.is_floating(target_type):
            return pa.scalar(float(value), type=target_type)
        if pa.types.is_boolean(target_type):
            if isinstance(value, str):
                lowered = value.lower()
                if lowered in {"true", "1"}:
                    value = True
                elif lowered in {"false", "0"}:
                    value = False
                else:
                    raise ValueError
            return pa.scalar(bool(value), type=target_type)
        if pa.types.is_string(target_type) or pa.types.is_large_string(target_type):
            return pa.scalar(str(value), type=target_type)
    except (TypeError, ValueError):
        raise ValueError(
            f"Value {value!r} is incompatible with column type {target_type}."
        )
    raise ValueError(f"Column type {target_type} is not supported in WHERE clause.")


def _apply_where(table: pa.Table, where_clause: Optional[str]) -> pa.Table:
    if not where_clause:
        return table

    column_name, op, raw_value = _parse_where_clause(where_clause)
    if column_name not in table.column_names:
        raise ValueError(f"Column '{column_name}' not found in dataset.")

    column = table[column_name]
    scalar = _coerce_scalar(column, _parse_literal(raw_value))

    if op == "=":
        mask = pc.equal(column, scalar)
    elif op == "!=":
        mask = pc.not_equal(column, scalar)
    elif op == ">":
        mask = pc.greater(column, scalar)
    elif op == "<":
        mask = pc.less(column, scalar)
    elif op == ">=":
        mask = pc.greater_equal(column, scalar)
    elif op == "<=":
        mask = pc.less_equal(column, scalar)
    else:
        raise ValueError(f"Unsupported operator '{op}'.")

    return table.filter(mask)


def _select_columns(table: pa.Table, columns: List[str]) -> pa.Table:
    if columns == ["*"]:
        return table

    cleaned = [_clean_identifier(col) for col in columns]
    for name in cleaned:
        if name not in table.column_names:
            raise ValueError(f"Column '{name}' not found in dataset.")
    return table.select(cleaned)


def _format_csv(table: pa.Table, config: OutputConfig) -> bytes:
    rows = table.to_pylist()
    buffer = io.StringIO()
    writer = csv.writer(
        buffer,
        delimiter=config.field_delimiter,
        lineterminator=config.record_delimiter,
    )
    for row in rows:
        writer.writerow(
            [
                "" if value is None else value
                for value in (row.get(col) for col in table.column_names)
            ]
        )
    return buffer.getvalue().encode("utf-8")


def _format_json(table: pa.Table, config: OutputConfig) -> bytes:
    rows = table.to_pylist()
    if config.json_type.upper() == "DOCUMENT":
        return json.dumps(rows).encode("utf-8")

    delimiter = config.record_delimiter or "\n"
    payload = delimiter.join(json.dumps(row) for row in rows)
    if payload and not payload.endswith(delimiter):
        payload += delimiter
    return payload.encode("utf-8")


def _encode_headers(headers: Dict[str, str]) -> bytes:
    encoded = bytearray()
    for name, value in headers.items():
        name_bytes = name.encode("utf-8")
        value_bytes = value.encode("utf-8")
        encoded.append(len(name_bytes))
        encoded.extend(name_bytes)
        encoded.append(7)  # header value type = string
        encoded.extend(struct.pack(">H", len(value_bytes)))
        encoded.extend(value_bytes)
    return bytes(encoded)


def _event_message(
    event_type: str, payload: bytes = b"", content_type: Optional[str] = None
) -> bytes:
    headers = {
        ":message-type": "event",
        ":event-type": event_type,
    }
    if content_type:
        headers[":content-type"] = content_type

    header_bytes = _encode_headers(headers)
    total_length = 4 + 4 + 4 + len(header_bytes) + len(payload) + 4
    prelude = struct.pack(">II", total_length, len(header_bytes))
    prelude_crc = struct.pack(">I", zlib.crc32(prelude) & 0xFFFFFFFF)
    data = header_bytes + payload
    message_crc = struct.pack(
        ">I", zlib.crc32(prelude + prelude_crc + data) & 0xFFFFFFFF
    )
    return prelude + prelude_crc + data + message_crc


def _validate_input_serialization(root: ET.Element) -> Optional[Response]:
    input_serialization = root.find(_ns("InputSerialization"))
    if input_serialization is None:
        return Response(
            content="InputSerialization element is required for SelectObjectContent.",
            status_code=400,
            media_type="text/plain",
        )

    if input_serialization.find(_ns("Parquet")) is None:
        return Response(
            content="Only Parquet input is supported by this emulator.",
            status_code=400,
            media_type="text/plain",
        )
    return None


def _parse_output_serialization(root: ET.Element) -> OutputConfig:
    output_serialization = root.find(_ns("OutputSerialization"))
    if output_serialization is None:
        return OutputConfig(format="json")

    csv_elem = output_serialization.find(_ns("CSV"))
    if csv_elem is not None:
        field_delimiter = _decode_text(csv_elem.find(_ns("FieldDelimiter"))) or ","
        record_delimiter = _decode_text(csv_elem.find(_ns("RecordDelimiter"))) or "\n"
        return OutputConfig(
            format="csv",
            field_delimiter=field_delimiter,
            record_delimiter=record_delimiter,
        )

    json_elem = output_serialization.find(_ns("JSON"))
    if json_elem is not None:
        json_type = _decode_text(json_elem.find(_ns("Type"))) or "LINES"
        record_delimiter = _decode_text(json_elem.find(_ns("RecordDelimiter"))) or "\n"
        return OutputConfig(
            format="json",
            json_type=json_type,
            record_delimiter=record_delimiter,
        )

    return OutputConfig(format="json")


def _format_output(table: pa.Table, config: OutputConfig) -> bytes:
    if config.format == "csv":
        return _format_csv(table, config)
    return _format_json(table, config)


def SelectObjectContent(bucket: str, object: str, body: bytes):
    """
    Handle S3 Select queries on objects.
    """
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        return Response(
            content=f"Error parsing request body: {exc}",
            status_code=400,
            media_type="text/plain",
        )

    validation_error = _validate_input_serialization(root)
    if validation_error is not None:
        return validation_error

    expression = _decode_text(root.find(_ns("Expression")))
    if expression is None:
        return Response(
            content="Expression element is required.",
            status_code=400,
            media_type="text/plain",
        )

    output_config = _parse_output_serialization(root)

    blob_content = get_blob_content(bucket=bucket, blob_name=object)
    if blob_content is None:
        return Response(
            content="Object not found",
            status_code=404,
            media_type="text/plain",
        )

    try:
        table = pq.read_table(io.BytesIO(blob_content))
        select_columns, where_clause = _parse_sql(expression)
        table = _apply_where(table, where_clause)
        table = _select_columns(table, select_columns)
        payload = _format_output(table, output_config)
    except Exception as exc:  # noqa: BLE001 - return user-friendly message
        return Response(
            content=f"Error executing query: {exc}",
            status_code=400,
            media_type="text/plain",
        )

    def _stream() -> Iterable[bytes]:
        if payload:
            yield _event_message("Records", payload, "application/octet-stream")
        yield _event_message("End")

    return StreamingResponse(_stream(), media_type="application/octet-stream")
