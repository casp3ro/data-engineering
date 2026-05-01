# 🚗 Car Price Pipeline

End-to-end data engineering project: ingest ~350k+ car listings (Craigslist), process them through a Medallion Architecture (Bronze → Silver → Gold), and serve insights via an interactive dashboard.

**Runs fully locally on Docker** — no cloud account needed.

---

## TL;DR (what to look at)

- **Dashboard**: `http://localhost:8501` (Streamlit + Plotly)
- **Airflow**: `http://localhost:8080` (DAG orchestration)
- **MinIO console**: `http://localhost:9001` (S3-compatible object store)
- **Kafka UI**: `http://localhost:8085` (streaming / topic inspection)
- **Other exposed ports** (from `docker-compose.yml`): Kafka `9092`, Spark master `7077`, Spark Thrift `10000`
- Deep dives: `docs/ARCHITECTURE.md`, `SECURITY.md`, `CONTRIBUTING.md`

## Demo in 2 minutes

Prereqs: Docker + Docker Compose, Python 3.11, Java (JDK), `uv`.

```bash
docker compose up -d
uv sync

export SPARK_MASTER_URL="local[*]"
export MINIO_S3_ENDPOINT="http://localhost:9000"

uv run python scripts/run_all.py
uv run streamlit run src/interfaces/dashboard/app.py  # don't run if using the `dashboard` service in Docker (same :8501)
```

**You should see**:

- Airflow UI at `http://localhost:8080`
- Streamlit dashboard at `http://localhost:8501`

## Screenshots

- Dashboard: `docs/assets/dashboard.png` (add screenshot)
- Airflow DAG: `docs/assets/airflow_dag.png` (add screenshot)

---

## Architecture

```
vehicles.csv
     │
     ▼
┌─────────────────────────────────────┐
│  BRONZE  · Apache Spark             │  Raw Delta Lake on MinIO (S3-compatible)
│  s3a://bronze/listings              │  Immutable, append-only
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│  SILVER  · Apache Spark             │  Cleaned, validated, deduplicated
│  data/silver/listings               │  Parquet + business rule filters
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│  GOLD    · dbt + DuckDB             │  Aggregated marts ready for BI
│  data/warehouse.duckdb              │  Median price by make / year / state
└─────────────────────────────────────┘
     │
     ▼
📊 Streamlit Dashboard · http://localhost:8501
```

---

## Stack

| Layer              | Technology                         |
| ------------------ | ---------------------------------- |
| Ingestion          | Apache Spark (PySpark)             |
| Storage            | Delta Lake + MinIO (S3-compatible) |
| Transformation     | Apache Spark + dbt-duckdb          |
| Serving            | DuckDB                             |
| Orchestration      | Apache Airflow                     |
| Streaming          | Apache Kafka + Zookeeper           |
| Dashboard          | Streamlit + Plotly                 |
| Containerization   | Docker Compose                     |
| Data validation    | Pydantic v2                        |
| Testing            | Pytest + dbt tests                 |
| Package management | uv                                 |

---

## Dashboard

- **KPI metrics** — total listings, unique makes, states covered, median price
- **Bar chart** — median price by make (top 20)
- **Line chart** — price depreciation curve by year per make
- **Choropleth map** — median price by US state

---

## Dataset

[Craigslist Cars & Trucks — Austin Reese](https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data)

~426k rows, 26 columns. Download `vehicles.csv` and place in `data/raw/vehicles.csv`.

---

## Run modes

### Quick demo (batch)

Best for first-time users. Runs the batch pipeline locally (CSV → Bronze → Silver → dbt → DuckDB) and opens the dashboard.

```bash
docker compose up -d
uv sync

export SPARK_MASTER_URL="local[*]"
export MINIO_S3_ENDPOINT="http://localhost:9000"

uv run python scripts/run_all.py
uv run streamlit run src/interfaces/dashboard/app.py  # don't run if using the `dashboard` service in Docker (same :8501)
```

### Full demo (end-to-end: streaming + orchestration)

Runs the end-to-end DAG via Airflow (Kafka produce → Spark Structured Streaming to Bronze → Silver → dbt run/test).

```bash
docker compose up -d
```

Then:

- Open **Airflow** at `http://localhost:8080` and trigger `car_price_pipeline`
- Inspect Kafka messages in **Kafka UI** at `http://localhost:8085`
- Inspect objects in **MinIO console** at `http://localhost:9001`
- Open **Dashboard** at `http://localhost:8501`

---

## Quick Start

### Requirements

- Docker + Docker Compose
- Python 3.11
- Java (JDK) — required by PySpark: `brew install --cask temurin`
- uv: `pip install uv`

### 1. Start infrastructure

```bash
docker compose up -d
```

Services started:

- MinIO (S3 API) → http://localhost:9000
- MinIO Console → http://localhost:9001
- Apache Spark → http://localhost:8081
- Apache Airflow → http://localhost:8080
- Kafka UI → http://localhost:8085

### 2. Install dependencies

```bash
uv sync
```

### 3. Run the pipeline

```bash
export SPARK_MASTER_URL="local[*]"
export MINIO_S3_ENDPOINT="http://localhost:9000"

uv run python scripts/run_all.py
```

Or step by step:

```bash
uv run python scripts/setup_minio.py       # create MinIO buckets
uv run python scripts/ingest_to_bronze.py  # CSV → Bronze Delta (Spark)
uv run python scripts/transform_silver.py  # Bronze → Silver (Spark)
uv run python scripts/run_dbt.py           # Silver → Gold (dbt + DuckDB)
```

### 4. Launch dashboard

```bash
uv run streamlit run src/interfaces/dashboard/app.py
```

→ http://localhost:8501

---

## Project Structure

```
├── src/
│   ├── domain/              # Business entities — Listing, Price, Mileage
│   ├── application/         # Use cases — ingest, stream, transform
│   ├── infrastructure/      # Spark, Kafka, MinIO clients
│   └── interfaces/          # Streamlit dashboard, CLI
├── dbt/
│   ├── models/
│   │   ├── silver/          # stg_listings, int_listings_valid
│   │   └── gold/            # mart_price_by_make/year/state, mart_listings_summary
│   └── macros/              # clean_string
├── scripts/
│   ├── run_all.py           # Full pipeline in one command
│   ├── setup_minio.py       # Create Bronze/Silver/Gold buckets
│   ├── ingest_to_bronze.py  # Spark CSV → Delta Lake
│   ├── transform_silver.py  # Spark Bronze → Silver
│   └── run_dbt.py           # dbt run + dbt test
├── dags/
│   └── car_price_pipeline.py  # Airflow DAG
└── docker-compose.yml
```

---

## dbt Models

```
stg_listings          view    — raw Silver data, cleaned strings
int_listings_valid    table   — filtered by business rules (price $500–$200k, year 1990–2024)
mart_price_by_make    table   — median/avg/min/max price per make (min 50 listings)
mart_price_by_year    table   — price depreciation curve, top 10 makes
mart_price_by_state   table   — median price per US state
mart_listings_summary table   — single-row KPI summary
```

### dbt tests

```
assert_price_positive       — no negative prices
not_null_stg_listings_id    — id always present
not_null_stg_listings_make  — make always present
not_null_stg_listings_price — price always present
unique_stg_listings_id      — no duplicate listings
```

---

## Design Patterns

- **Medallion Architecture** — Bronze (raw) → Silver (clean) → Gold (aggregated)
- **Domain-Driven Design** — `Listing`, `Price`, `Mileage` as domain entities with Pydantic validation
- **Repository Pattern** — storage abstraction decoupled from business logic
- **Idempotency** — all pipeline steps safe to re-run

---

## Links

- Dataset: [Craigslist Cars & Trucks — Austin Reese](https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data)
- Delta Lake: https://delta.io
- dbt docs: https://docs.getdbt.com
- MinIO: https://min.io
- fast.ai (ML companion): https://course.fast.ai
