"""
GetObject

https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
"""

from fastapi import Request, Response

from .storage import get_blob_content


def GetObject(bucket: str, object: str, request: Request):
    content = get_blob_content(bucket=bucket, blob_name=object)

    if content is None:
        return Response(
            content="Object not found", status_code=404, media_type="text/plain"
        )

    return Response(
        content,
        media_type="application/octet-stream",
    )
