import logging
import os

import uvicorn
from fastapi import FastAPI, Request, Response

from services import GetBucketLocation, GetObject, ListObjects, SelectObjectContent

application = FastAPI(title="S1")
logging = logging.getLogger("S1")


@application.get("/{bucket}")
def bucket_request_handler(request: Request, bucket: str):
    # Check for GetBucketLocation request (/?location query parameter)
    if "location" in request.query_params:
        return GetBucketLocation()

    # For ListObjects, we always call it since it handles all query parameters
    # including delimiter, prefix, max-keys, etc.
    return ListObjects(bucket, request)


@application.get("/{bucket}/{object_key:path}")
def object_request_handler(bucket: str, object_key: str, request: Request):
    print("GetObject")
    return GetObject(bucket, object_key, request)


@application.post("/{bucket}/{object_key:path}")
async def object_post_handler(bucket: str, object_key: str, request: Request):
    # S3 Select uses POST with 'select' and 'select-type' query parameters
    if "select" in request.query_params:
        print("SelectObjectContent")
        body = await request.body()
        return SelectObjectContent(bucket, object_key, body)

    return Response(
        content="Unsupported POST operation", status_code=400, media_type="text/plain"
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:application",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        log_level="info",
        access_log=True,
    )
