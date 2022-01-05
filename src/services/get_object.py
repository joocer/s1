"""
GetObject

https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
"""

from fastapi import Response, Request


def GetObject(bucket: str, object: str, request: Request):

    return Response(
        b'abcdefghijklmnopqrstuvwxyz',
        media_type="application/octet-stream",
    )
