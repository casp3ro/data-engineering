docker compose up -d
## Requirements
- Install a Java runtime (JDK) on your host (PySpark needs it).
  - Example: `brew install --cask temurin`

## Run (CSV → Bronze → Silver → dbt)
export SPARK_MASTER_URL="local[*]"
export MINIO_S3_ENDPOINT="http://localhost:9000"
uv run python scripts/run_all.py
cd dbt && dbt run --profiles-dir . && dbt test --profiles-dir .
uv run streamlit run src/interfaces/dashboard/app.py
