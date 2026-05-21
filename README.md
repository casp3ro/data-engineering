# Car Price Pipeline

End-to-end data engineering project: ingest ~350k Craigslist vehicle listings via Kafka, process through a Medallion Architecture on Databricks, and serve insights from a Streamlit dashboard.

## Architecture

```
vehicles.csv
    │
    ▼
[Kafka Producer] ──► [Kafka Topic: car-listings]
                               │
                               ▼
                    [Databricks Job (DBR 14.3)]
                    ┌──────────────────────────┐
                    │  01_bronze_ingest         │  Kafka → Bronze Delta (DBFS)
                    │  02_silver_transform      │  Clean + filter + deduplicate
                    │  03_gold_aggregate        │  Business aggregations
                    └──────────────────────────┘
                               │
                               ▼
                    [dbt-duckdb] ──► [warehouse.duckdb]
                                              │
                                              ▼
                                    [Streamlit Dashboard]
```

Orchestrated by **Apache Airflow** (daily schedule, paused by default).

## Stack

| Tool | Role | Version |
|------|------|---------|
| Apache Kafka | Event streaming — transport | 7.6.0 |
| Avro / fastavro | Schema contract for events | 1.9.x |
| Databricks | Distributed compute (Bronze/Silver/Gold) | DBR 14.3 LTS |
| Delta Lake | Storage format — ACID, time travel | 3.x |
| Apache Airflow | Orchestration — DAG, scheduling | 2.8.0 |
| dbt-duckdb | SQL transformations, tests, lineage | 1.8.x |
| DuckDB | Serving layer — analytics queries | 0.10+ |
| Streamlit | Dashboard — end-user visualisation | 1.30+ |
| Docker Compose | Local infra — Kafka + Airflow + Streamlit | — |
| GitHub Actions | CI — lint, unit tests, dbt parse | — |

## Quick Start

### Prerequisites

- Docker + Docker Compose
- Python 3.11+ and `uv`
- Databricks workspace (free trial at databricks.com)
- Kaggle account (for dataset download)

### 1. Clone and configure

```bash
git clone https://github.com/casp3ro/data-engineering.git
cd data-engineering
cp infra/.env.example .env
# Edit .env — fill in DATABRICKS_HOST, DATABRICKS_TOKEN, and generate FERNET_KEY
```

Generate required secrets:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# paste output as AIRFLOW__CORE__FERNET_KEY
python -c "import secrets; print(secrets.token_hex(32))"
# paste output as AIRFLOW__WEBSERVER__SECRET_KEY
```

### 2. Download dataset

Download `vehicles.csv` from [Kaggle Craigslist Cars & Trucks](https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data)
and place it at `data/raw/vehicles.csv`.

### 3. Set up Databricks

```bash
# Install Databricks CLI
pip install databricks-cli
databricks configure --token

# Upload notebooks
databricks workspace import_dir databricks/notebooks /Repos/car-price-pipeline/databricks/notebooks --overwrite

# Upload Avro schema to DBFS
databricks fs cp kafka/schemas/listing.avsc dbfs:/pipelines/car-price/schemas/listing.avsc

# Register the pipeline job
JOB_ID=$(databricks jobs create --json-file databricks/jobs/pipeline_job.json | jq -r '.job_id')
echo "DATABRICKS_JOB_ID=$JOB_ID" >> .env
```

Set up the Databricks connection in Airflow after starting the stack:
- Connection ID: `databricks_default`
- Type: `Databricks`
- Host: your workspace URL
- Token: your PAT

Also add an Airflow Variable:
- `DATABRICKS_JOB_ID`: the job ID from above
- `CAR_PRICE_CSV_PATH`: `/opt/airflow/data/raw/vehicles.csv`

### 4. Start local infrastructure

```bash
docker compose -f infra/docker-compose.yml up -d
```

Services:
- Airflow UI: http://localhost:8080 (admin/admin)
- Kafka UI: http://localhost:8085
- Dashboard: http://localhost:8501

### 5. Run the pipeline

Via Airflow UI: unpause `car_price_pipeline` and trigger manually.

Or manually:
```bash
uv sync
# Produce to Kafka
uv run python -c "
from pathlib import Path
from kafka.config import KafkaConfig
from kafka.producer import ListingProducer
p = ListingProducer(KafkaConfig())
print(p.produce_from_csv(Path('data/raw/vehicles.csv')))
p.close()
"
# Then trigger the Databricks job via the workspace UI
```

## Pipeline Flow

| Step | What happens | Typical duration |
|------|-------------|-----------------|
| `produce_to_kafka` | Reads CSV, validates ~350k rows via domain rules, publishes Avro events | ~3 min |
| `wait_for_kafka` | 30s settle time for Kafka offsets | 30s |
| `databricks_pipeline` | Bronze ingest → Silver clean → Gold aggregate on Databricks | ~8–15 min |
| `dbt_run` | Reads Gold Delta, materialises DuckDB marts | ~30s |
| `dbt_test` | Runs data quality tests | ~15s |

## Development

```bash
uv sync --extra dev

# Run unit tests (no external services needed)
uv run pytest tests/unit/ -v

# Run integration tests (requires running Kafka)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest tests/integration/ -m integration

# Lint + type check
uv run ruff check .
uv run mypy src/ kafka/

# dbt
cd dbt && uv run dbt parse && uv run dbt compile
```

### Adding a new Gold model

1. Add the aggregation to `databricks/notebooks/03_gold_aggregate.py`
2. Add a staging model in `dbt/models/staging/stg_<name>.sql`
3. Add a mart model in `dbt/models/marts/mart_<name>.sql`
4. Add column tests in the model's `.yml` file
5. Add a singular test in `dbt/tests/`

### Naming conventions

- dbt staging: `stg_<gold_table_name>.sql`
- dbt marts: `mart_<business_concept>.sql`
- Databricks notebooks: `NN_<layer>_<verb>.py`
- Airflow tasks: `snake_case` verb-noun

## Architecture Decisions

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for full ADRs.

- **Kafka over direct file read**: decouples ingestion from processing, enforces schema contract via Avro
- **Databricks over local Spark**: eliminates JVM/Maven setup, matches production DE tooling
- **DuckDB as serving layer**: zero-server OLAP, reads Delta directly via `delta_scan()`, fast for dashboards
- **dbt over notebook transforms**: version-controlled SQL, lineage, column tests, readable docs
