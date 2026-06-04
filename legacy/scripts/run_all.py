"""
scripts/run_all.py
Run the full local pipeline in order:
  1. Setup MinIO buckets
  2. Ingest CSV → Bronze Delta
  3. Transform Bronze → Silver Delta
  4. dbt run + dbt test → DuckDB warehouse
"""
import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

SCRIPTS = Path(__file__).parent


def run(script: str) -> None:
    path = SCRIPTS / script
    logger.info("--- %s ---", script)
    result = subprocess.run([sys.executable, str(path)])
    if result.returncode != 0:
        logger.error("FAILED: %s exited with code %d", script, result.returncode)
        sys.exit(result.returncode)
    logger.info("OK: %s", script)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    logger.info("Car Price Pipeline — Full Local Run")
    logger.info("Ensure Docker is up: docker compose -f infra/docker-compose.yml --profile local up -d")

    run("setup_minio.py")
    run("ingest_to_bronze.py")
    run("transform_silver.py")
    run("run_dbt.py")

    logger.info("Pipeline complete.")
    logger.info("Dashboard: http://localhost:8501")
    logger.info("MinIO:     http://localhost:9001")
    logger.info("Airflow:   http://localhost:8080")


if __name__ == "__main__":
    main()
