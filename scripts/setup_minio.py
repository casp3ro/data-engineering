from src.infrastructure.storage.minio_client import MinioClient

if __name__ == "__main__":
    client = MinioClient()
    client.setup_buckets()
    print("MinIO setup complete.")
