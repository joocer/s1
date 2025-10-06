"""
ListObjects

https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html

"""
import os
from fastapi import Response, Request
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials


def get_blobs_at_path(bucket, path):

    # this means we're testing
    if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
        client = storage.Client(
            credentials=AnonymousCredentials(),
            project="PROJECT",
        )
    else:  # pragma: no cover
        client = storage.Client(project="PROJECT")

    gcs_bucket = client.get_bucket(bucket)
    return list(client.list_blobs(bucket_or_name=gcs_bucket, prefix=path))



def ListObjects(bucket: str, request: Request):
    # Get query parameters
    prefix = request.query_params.get("prefix", "")
    delimiter = request.query_params.get("delimiter", "")
    max_keys = int(request.query_params.get("max-keys", "1000"))
    marker = request.query_params.get("marker", "")
    
    # Get blobs from GCS
    blobs = get_blobs_at_path(bucket, prefix)
    
    # Build XML response
    xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml_parts.append('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">')
    xml_parts.append(f'    <Name>{bucket}</Name>')
    xml_parts.append(f'    <Prefix>{prefix}</Prefix>')
    xml_parts.append(f'    <Marker>{marker}</Marker>')
    xml_parts.append(f'    <MaxKeys>{max_keys}</MaxKeys>')
    xml_parts.append('    <IsTruncated>false</IsTruncated>')
    
    for blob in blobs[:max_keys]:
        xml_parts.append('    <Contents>')
        xml_parts.append(f'        <Key>{blob.name}</Key>')
        xml_parts.append(f'        <LastModified>{blob.updated.isoformat()}</LastModified>')
        xml_parts.append(f'        <ETag>"{blob.etag}"</ETag>')
        xml_parts.append(f'        <Size>{blob.size}</Size>')
        xml_parts.append('        <StorageClass>STANDARD</StorageClass>')
        xml_parts.append('    </Contents>')
    
    xml_parts.append('</ListBucketResult>')
    
    return Response(
        '\n'.join(xml_parts).encode(),
        media_type="application/xml",
    )
