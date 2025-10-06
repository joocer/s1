import os
import uvicorn
from fastapi import FastAPI
from fastapi import Request, Response
import logging

from services import GetBucketLocation, ListObjects, GetObject, SelectObjectContent


application = FastAPI(title="S1")
logging = logging.getLogger("S1")


@application.get("/{bucket}")
def bucket_request_handler(bucket: str, request: Request):

    if "location" in request.query_params:
        print("GetBucketLocation")
        return GetBucketLocation()

    if "delimiter" in request.query_params:
        print("ListObjects")
        return ListObjects(bucket, request)

@application.get("/{bucket}/{object:path}")
def object_request_handler(bucket: str, object: str, request: Request):
        print("GetObject")
        return GetObject(bucket, object, request)

@application.post("/{bucket}/{object:path}")
async def object_post_handler(bucket: str, object: str, request: Request):
    # S3 Select uses POST with 'select' and 'select-type' query parameters
    if "select" in request.query_params:
        print("SelectObjectContent")
        body = await request.body()
        return SelectObjectContent(bucket, object, body)
    
    return Response(
        content="Unsupported POST operation",
        status_code=400,
        media_type="text/plain"
    )

if __name__ == "__main__":

    uvicorn.run(
        "main:application",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        lifespan="on",
    )
