"""
ListObjects

https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html

"""

from fastapi import Request, Response

from .storage import list_blobs


def ListObjects(bucket: str, request: Request):
    # Get query parameters
    prefix = request.query_params.get("prefix", "")
    max_keys = int(request.query_params.get("max-keys", "1000"))
    marker = request.query_params.get("marker", "")
    start_after = request.query_params.get("start-after")
    continuation_token = request.query_params.get("continuation-token")
    delimiter = request.query_params.get("delimiter", "")

    # Get blobs from storage backend
    blobs = list_blobs(bucket, prefix)

    # Apply marker/start-after semantics for pagination compatibility
    pivot = start_after or continuation_token or marker
    if pivot:
        blobs = [blob for blob in blobs if blob.name > pivot]

    # Build XML response
    xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml_parts.append(
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    )
    xml_parts.append(f"    <Name>{bucket}</Name>")
    xml_parts.append(f"    <Prefix>{prefix}</Prefix>")
    xml_parts.append(f"    <Marker>{marker}</Marker>")
    if start_after:
        xml_parts.append(f"    <StartAfter>{start_after}</StartAfter>")
    xml_parts.append(f"    <MaxKeys>{max_keys}</MaxKeys>")
    xml_parts.append(f"    <KeyCount>{min(len(blobs), max_keys)}</KeyCount>")
    if delimiter is not None:
        xml_parts.append(f"    <Delimiter>{delimiter}</Delimiter>")
    xml_parts.append("    <IsTruncated>false</IsTruncated>")

    for blob in blobs[:max_keys]:
        xml_parts.append("    <Contents>")
        xml_parts.append(f"        <Key>{blob.name}</Key>")
        xml_parts.append(f"        <LastModified>{blob.updated}</LastModified>")
        xml_parts.append(f"        <ETag>{blob.etag}</ETag>")
        xml_parts.append(f"        <Size>{blob.size}</Size>")
        xml_parts.append("        <StorageClass>STANDARD</StorageClass>")
        xml_parts.append("    </Contents>")

    xml_parts.append("</ListBucketResult>")

    return Response(
        "\n".join(xml_parts).encode(),
        media_type="application/xml",
    )
