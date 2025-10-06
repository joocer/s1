"""
GetBucketLocation

https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html

Returns the Region the bucket resides in. We're going to return London (eu-west-2) to
all requests.

For more information on AWS locations, see:
https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
"""

from fastapi import Response


def GetBucketLocation():
    return Response(
        content="""<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">eu-west-2</LocationConstraint>""",
        media_type="application/xml",
    )
