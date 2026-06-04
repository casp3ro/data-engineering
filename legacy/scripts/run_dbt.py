"""
scripts/run_dbt.py — run dbt against the local MinIO silver path.
"""
import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

DBT_DIR = Path(__file__).parent.parent / "dbt"
DBT_TARGET = os.getenv("DBT_TARGET", "local")


def run(cmd: str) -> None:
    logger.info("$ %s", cmd)
    env = {**os.environ, "PIPELINE_MODE": "local"}
    result = subprocess.run(cmd, shell=True, cwd=DBT_DIR, env=env)
    if result.returncode != 0:
        logger.error("Command failed with code %d", result.returncode)
        sys.exit(result.returncode)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    run(f"dbt run --profiles-dir . --target {DBT_TARGET}")
    run(f"dbt test --profiles-dir . --target {DBT_TARGET}")
    logger.info("dbt complete.")


if __name__ == "__main__":
    main()
