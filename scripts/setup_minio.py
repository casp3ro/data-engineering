from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def main() -> None:
    # Allow running as a script from the repo root without requiring users to set PYTHONPATH.
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from src.infrastructure.storage.minio_client import MinioClient

    client = MinioClient()
    client.setup_buckets()
    print("MinIO setup complete.")


if __name__ == "__main__":
    main()
