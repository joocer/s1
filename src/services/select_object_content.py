"""
SelectObjectContent

https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html

This service enables applications to filter the contents of an Amazon S3 object
using simple SQL expressions.
"""

import csv
import io
import json
from xml.etree import ElementTree as ET

from fastapi import Response

from .storage import get_blob_content


def parse_sql_expression(sql: str):
    """
    Simple SQL parser for S3 Select queries.
    Supports basic SELECT statements.
    """
    # This is a simplified parser for demo purposes
    # A production implementation would use a proper SQL parser
    sql = sql.strip().upper()

    # Extract SELECT columns
    select_idx = sql.find("SELECT")
    from_idx = sql.find("FROM")

    if select_idx == -1 or from_idx == -1:
        raise ValueError("Invalid SQL: must contain SELECT and FROM")

    select_part = sql[select_idx + 6 : from_idx].strip()

    # Extract WHERE clause if present
    where_idx = sql.find("WHERE")
    where_clause = None
    if where_idx != -1:
        where_clause = sql[where_idx + 5 :].strip()

    return select_part, where_clause


def apply_sql_to_csv(content: bytes, sql_expression: str):
    """
    Apply SQL expression to CSV content.
    """
    select_part, where_clause = parse_sql_expression(sql_expression)

    # Parse CSV
    csv_text = content.decode("utf-8")
    csv_reader = csv.DictReader(io.StringIO(csv_text))

    results = []
    for row in csv_reader:
        # Apply WHERE clause if present (simplified)
        if where_clause:
            # This is very simplified - a real implementation would parse properly
            # For now, we'll just return all rows
            pass

        # Apply SELECT
        if select_part == "*":
            results.append(row)
        else:
            # Extract specified columns
            columns = [col.strip() for col in select_part.split(",")]
            filtered_row = {col: row.get(col, "") for col in columns if col in row}
            results.append(filtered_row)

    return results


def apply_sql_to_json(content: bytes, sql_expression: str):
    """
    Apply SQL expression to JSON content.
    """
    select_part, where_clause = parse_sql_expression(sql_expression)

    # Parse JSON
    data = json.loads(content.decode("utf-8"))

    # Handle both single object and array of objects
    if not isinstance(data, list):
        data = [data]

    results = []
    for item in data:
        # Apply WHERE clause if present (simplified)
        if where_clause:
            pass

        # Apply SELECT
        if select_part == "*":
            results.append(item)
        else:
            columns = [col.strip() for col in select_part.split(",")]
            filtered_item = {col: item.get(col) for col in columns if col in item}
            results.append(filtered_item)

    return results


def SelectObjectContent(bucket: str, object: str, body: bytes):
    """
    Handle S3 Select queries on objects.
    """
    # Parse the request body to get SQL expression and input/output format
    # For S3 Select, the request body is XML containing:
    # - Expression (SQL query)
    # - ExpressionType (SQL)
    # - InputSerialization (Parquet format only - CSV and JSON not supported)
    # - OutputSerialization (CSV or JSON format details)

    # Parse XML request body
    try:
        root = ET.fromstring(body)

        # Extract input serialization format - only Parquet is supported for SQL API
        input_serialization = root.find(
            ".//{http://s3.amazonaws.com/doc/2006-03-01/}InputSerialization"
        )

        if input_serialization is None:
            return Response(
                content="SQL API only supports Parquet files. InputSerialization element is required.",
                status_code=400,
                media_type="text/plain",
            )

        parquet_elem = input_serialization.find(
            ".//{http://s3.amazonaws.com/doc/2006-03-01/}Parquet"
        )
        csv_elem = input_serialization.find(
            ".//{http://s3.amazonaws.com/doc/2006-03-01/}CSV"
        )
        json_elem = input_serialization.find(
            ".//{http://s3.amazonaws.com/doc/2006-03-01/}JSON"
        )

        if parquet_elem is not None:
            input_format = "Parquet"
        elif csv_elem is not None or json_elem is not None:
            # CSV and JSON are not supported for SQL API
            format_name = "CSV" if csv_elem is not None else "JSON"
            return Response(
                content=f"SQL API only supports Parquet files. {format_name} format is not supported. Use blob access (GetObject) for other file types.",
                status_code=400,
                media_type="text/plain",
            )
        else:
            return Response(
                content="SQL API only supports Parquet files. Please specify Parquet in InputSerialization.",
                status_code=400,
                media_type="text/plain",
            )

    except Exception as e:
        return Response(
            content=f"Error parsing request: {str(e)}",
            status_code=400,
            media_type="text/plain",
        )

    # Get the object from storage
    content = get_blob_content(bucket=bucket, blob_name=object)

    if content is None:
        return Response(
            content="Object not found", status_code=404, media_type="text/plain"
        )

    # Apply SQL based on input format
    try:
        if input_format == "Parquet":
            # Parquet SQL processing would be implemented here
            # For now, return an error indicating Parquet SQL is not yet implemented
            return Response(
                content="Parquet SQL processing is not yet implemented",
                status_code=501,
                media_type="text/plain",
            )
        else:
            return Response(
                content=f"Unsupported input format: {input_format}. SQL API only supports Parquet files.",
                status_code=400,
                media_type="text/plain",
            )

    except Exception as e:
        return Response(
            content=f"Error executing query: {str(e)}",
            status_code=500,
            media_type="text/plain",
        )
