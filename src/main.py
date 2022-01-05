import os
import uvicorn
from fastapi import FastAPI
from fastapi import Request
import logging

from services import GetBucketLocation, ListObjects, GetObject


application = FastAPI(title="S1")
logging = logging.getLogger("S1")


@application.get("/{bucket}")
def bucket_request_handler(bucket: str, request: Request):

    if "location" in request.query_params:
        print("GetBucketLocation")
        return GetBucketLocation()

    if "delimiter" in request.query_params:
        print("ListObjects")
        return ListObjects("", request)

@application.get("/{bucket}/{object:path}")
def object_request_handler(bucket: str, object: str, request: Request):
        print("GetObject")
        return GetObject(bucket, object, request)

if __name__ == "__main__":

    uvicorn.run(
        "main:application",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        lifespan="on",
    )
