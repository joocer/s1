from minio import Minio  # type: ignore
from minio.select import (JSONOutputSerialization,  # type: ignore
                          ParquetInputSerialization, SelectRequest)

client = Minio(
    "localhost:8080",
    access_key="access_key",
    secret_key="secret_key",
    secure=False,
)

objects = list(client.list_objects("astronauts", recursive=True))
print("Objects:", [obj.object_name for obj in objects])

response = client.get_object("astronauts", "astronauts.parquet")
try:
    print("Parquet size:", len(response.read()))
finally:
    response.close()
    response.release_conn()

request = SelectRequest(
    "SELECT name, space_flights FROM S3Object WHERE space_flights > 2",
    ParquetInputSerialization(),
    JSONOutputSerialization(),
)

with client.select_object_content("astronauts", "astronauts.parquet", request) as reader:
    payload = b"".join(chunk for chunk in reader.stream())
    print("Select sample:", payload.decode("utf-8").splitlines()[:3])
