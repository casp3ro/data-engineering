from minio import Minio

BUCKETS = ["bronze", "silver", "gold"]


class MinioClient:
    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
    ) -> None:
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

    def setup_buckets(self) -> None:
        for bucket in BUCKETS:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
                print(f"Created bucket: {bucket}")
            else:
                print(f"Bucket exists: {bucket}")
