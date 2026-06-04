from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from src.infrastructure.storage.minio_client import MinioClient

    client = MinioClient()
    client.setup_buckets()
    logger.info("MinIO setup complete.")


if __name__ == "__main__":
    main()
