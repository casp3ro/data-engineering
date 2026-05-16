"""
scripts/run_dbt.py
"""
import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

DBT_DIR = Path(__file__).parent.parent / "dbt"


def run(cmd: str) -> None:
    logger.info("$ %s", cmd)
    result = subprocess.run(cmd, shell=True, cwd=DBT_DIR)
    if result.returncode != 0:
        logger.error("Command failed with code %d", result.returncode)
        sys.exit(result.returncode)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    run("dbt run --profiles-dir .")
    run("dbt test --profiles-dir .")
    logger.info("dbt complete.")


if __name__ == "__main__":
    main()
