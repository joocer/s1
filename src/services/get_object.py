"""
GetObject

https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
"""
import os
from fastapi import Response, Request
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials


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


def GetObject(bucket: str, object: str, request: Request):

    blob = get_blob("PROJECT", bucket=bucket, blob_name=object)
    
    if blob is None:
        return Response(
            content="Object not found",
            status_code=404,
            media_type="text/plain"
        )

    return Response(
        blob.download_as_bytes(),
        media_type="application/octet-stream",
    )
