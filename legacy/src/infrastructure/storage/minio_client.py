import logging
import os

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

BUCKETS = ["bronze", "silver", "gold", "warehouse"]


class MinioClient:
    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ) -> None:
        self._client = Minio(
            endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )

    def setup_buckets(self) -> None:
        for bucket in BUCKETS:
            try:
                if not self._client.bucket_exists(bucket):
                    self._client.make_bucket(bucket)
                    logger.info("Created bucket: %s", bucket)
                else:
                    logger.info("Bucket exists: %s", bucket)
            except S3Error as e:
                if e.code in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                    logger.info("Bucket exists: %s", bucket)
                    continue
                raise
