"""
SelectObjectContent

https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html

This service enables applications to filter the contents of an Amazon S3 object 
using simple SQL expressions.
"""
import os
import io
import json
import csv
from fastapi import Response, Request
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
from xml.etree import ElementTree as ET


def get_blob(project: str, bucket: str, blob_name: str):
    # this means we're testing
    if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
        client = storage.Client(
            credentials=AnonymousCredentials(),
            project=project,
        )
    else:  # pragma: no cover
        client = storage.Client(project=project)

    gcs_bucket = client.get_bucket(bucket)
    blob = gcs_bucket.get_blob(blob_name)
    return blob


def parse_sql_expression(sql: str):
    """
    Simple SQL parser for S3 Select queries.
    Supports basic SELECT statements.
    """
    # This is a simplified parser for demo purposes
    # A production implementation would use a proper SQL parser
    sql = sql.strip().upper()
    
    # Extract SELECT columns
    select_idx = sql.find('SELECT')
    from_idx = sql.find('FROM')
    
    if select_idx == -1 or from_idx == -1:
        raise ValueError("Invalid SQL: must contain SELECT and FROM")
    
    select_part = sql[select_idx + 6:from_idx].strip()
    
    # Extract WHERE clause if present
    where_idx = sql.find('WHERE')
    where_clause = None
    if where_idx != -1:
        where_clause = sql[where_idx + 5:].strip()
    
    return select_part, where_clause


def apply_sql_to_csv(content: bytes, sql_expression: str):
    """
    Apply SQL expression to CSV content.
    """
    select_part, where_clause = parse_sql_expression(sql_expression)
    
    # Parse CSV
    csv_text = content.decode('utf-8')
    csv_reader = csv.DictReader(io.StringIO(csv_text))
    
    results = []
    for row in csv_reader:
        # Apply WHERE clause if present (simplified)
        if where_clause:
            # This is very simplified - a real implementation would parse properly
            # For now, we'll just return all rows
            pass
        
        # Apply SELECT
        if select_part == '*':
            results.append(row)
        else:
            # Extract specified columns
            columns = [col.strip() for col in select_part.split(',')]
            filtered_row = {col: row.get(col, '') for col in columns if col in row}
            results.append(filtered_row)
    
    return results


def apply_sql_to_json(content: bytes, sql_expression: str):
    """
    Apply SQL expression to JSON content.
    """
    select_part, where_clause = parse_sql_expression(sql_expression)
    
    # Parse JSON
    data = json.loads(content.decode('utf-8'))
    
    # Handle both single object and array of objects
    if not isinstance(data, list):
        data = [data]
    
    results = []
    for item in data:
        # Apply WHERE clause if present (simplified)
        if where_clause:
            pass
        
        # Apply SELECT
        if select_part == '*':
            results.append(item)
        else:
            columns = [col.strip() for col in select_part.split(',')]
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
    # - InputSerialization (CSV, JSON, or Parquet format details)
    # - OutputSerialization (CSV or JSON format details)
    
    # Parse XML request body
    try:
        root = ET.fromstring(body)
        
        # Extract SQL expression
        expression_elem = root.find('.//{http://s3.amazonaws.com/doc/2006-03-01/}Expression')
        expression = expression_elem.text if expression_elem is not None else "SELECT * FROM S3Object"
        
        # Extract input serialization format
        input_format_elem = root.find('.//{http://s3.amazonaws.com/doc/2006-03-01/}CSV')
        if input_format_elem is not None:
            input_format = 'CSV'
        else:
            json_elem = root.find('.//{http://s3.amazonaws.com/doc/2006-03-01/}JSON')
            input_format = 'JSON' if json_elem is not None else 'CSV'
        
        # Extract output serialization format
        output_csv = root.find('.//{http://s3.amazonaws.com/doc/2006-03-01/}OutputSerialization/{http://s3.amazonaws.com/doc/2006-03-01/}CSV')
        output_format = 'CSV' if output_csv is not None else 'JSON'
        
    except Exception as e:
        return Response(
            content=f"Error parsing request: {str(e)}",
            status_code=400,
            media_type="text/plain"
        )
    
    # Get the object from GCS
    blob = get_blob("PROJECT", bucket=bucket, blob_name=object)
    
    if blob is None:
        return Response(
            content="Object not found",
            status_code=404,
            media_type="text/plain"
        )
    
    # Download object content
    content = blob.download_as_bytes()
    
    # Apply SQL based on input format
    try:
        if input_format == 'CSV':
            results = apply_sql_to_csv(content, expression)
        elif input_format == 'JSON':
            results = apply_sql_to_json(content, expression)
        else:
            return Response(
                content=f"Unsupported input format: {input_format}",
                status_code=400,
                media_type="text/plain"
            )
        
        # Format output
        if output_format == 'CSV':
            output = io.StringIO()
            if results:
                writer = csv.DictWriter(output, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
            response_content = output.getvalue().encode()
            media_type = "text/csv"
        else:  # JSON
            response_content = json.dumps(results).encode()
            media_type = "application/json"
        
        return Response(
            content=response_content,
            media_type=media_type
        )
        
    except Exception as e:
        return Response(
            content=f"Error executing query: {str(e)}",
            status_code=500,
            media_type="text/plain"
        )
