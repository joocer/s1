from minio import Minio  # type:ignore


m = Minio("localhost:8080", "access_key", "secret_key", secure=False)
print(
    list(
        m.list_objects(
            bucket_name="gva", prefix="apples", recursive=True, start_after="app"
        )
    )
)

print(list(m.get_object("gva", "object_name", 1, 22)))