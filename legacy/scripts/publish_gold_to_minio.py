#!/usr/bin/env python3
"""
Verify Gold Delta tables exist on MinIO (exported from Databricks notebook 04).
No-op when tables are already present; logs missing paths for troubleshooting.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

GOLD_TABLES = (
    "price_by_brand",
    "price_by_year",
    "price_by_condition",
    "listings_summary",
    "price_by_state",
    "price_by_make_year",
)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from minio import Minio

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    client = Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )

    missing: list[str] = []
    for table in GOLD_TABLES:
        prefix = f"{table}/"
        objects = list(client.list_objects("gold", prefix=prefix, recursive=True))
        if not objects:
            missing.append(table)
        else:
            logger.info("gold/%s: %d objects", table, len(objects))

    if missing:
        logger.warning(
            "Missing Gold exports on MinIO: %s. "
            "Run Databricks notebook 04_export_gold_to_minio after notebook 03.",
            ", ".join(missing),
        )
        sys.exit(1)

    logger.info("All Gold tables present on MinIO.")


if __name__ == "__main__":
    main()
