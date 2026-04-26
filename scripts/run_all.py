"""
scripts/run_all.py
Run the full pipeline in order:
  1. Setup MinIO buckets
  2. Ingest CSV → Bronze Delta
  3. Transform Bronze → Silver Delta
  4. dbt run + dbt test → Gold Delta
"""
import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).parent


def run(script: str) -> None:
    path = SCRIPTS / script
    print(f"\n{'='*60}")
    print(f"Running: {script}")
    print('='*60)
    result = subprocess.run([sys.executable, str(path)])
    if result.returncode != 0:
        print(f"\nFAILED: {script} exited with code {result.returncode}")
        sys.exit(result.returncode)
    print(f"OK: {script}")


def main() -> None:
    print("Car Price Pipeline — Full Run")
    print("Make sure Docker is running: docker compose ps\n")

    run("setup_minio.py")
    run("ingest_to_bronze.py")
    run("transform_silver.py")
    run("run_dbt.py")

    print("\n" + "="*60)
    print("Pipeline complete.")
    print("Dashboard: http://localhost:8501")
    print("MinIO:     http://localhost:9001")
    print("Airflow:   http://localhost:8080")
    print("="*60)


if __name__ == "__main__":
    main()