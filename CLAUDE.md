# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Read `tasks/lessons.md` at the start of every session — it records hard-won patterns from past refactors on this codebase.

---

## Project

**Car Price Pipeline** — ingests ~350k Craigslist vehicle listings via Kafka, processes them through a Medallion Architecture (Bronze → Silver → Gold) on Databricks, and serves insights from a Streamlit dashboard. The serving layer is dbt-duckdb writing to `data/warehouse.duckdb`.

Orchestrated by **Apache Airflow** (daily, paused by default). Full pipeline run: ~12–18 min.

---

## Commands

```bash
# Install dependencies
uv sync --extra dev

# Lint
uv run ruff check .

# Type check
uv run mypy src/ kafka/ dags/ --ignore-missing-imports

# Unit tests (no external services needed — fast)
uv run pytest tests/unit/ -v --tb=short

# Spark tests (requires JVM — slow, ~3s session startup)
uv run pytest tests/ -m spark -v

# Integration tests (requires running Kafka at KAFKA_BOOTSTRAP_SERVERS)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest tests/integration/ -m integration

# dbt — run from the dbt/ subdirectory
cd dbt && uv run dbt parse --profiles-dir .
cd dbt && uv run dbt compile --profiles-dir .
cd dbt && uv run dbt run --profiles-dir .
cd dbt && uv run dbt test --profiles-dir .

# Start all local infrastructure (Kafka, Airflow, Streamlit, MinIO)
docker compose -f infra/docker-compose.yml up -d
# Airflow UI: http://localhost:8080 (admin/admin)
# Kafka UI:   http://localhost:8085
# Dashboard:  http://localhost:8501
```

---

## Architecture

```
vehicles.csv
    │
    ▼
[Kafka Producer] ──► [Kafka Topic: car-listings]  (Avro schema: kafka/schemas/listing.avsc)
                               │
                               ▼
                    [Databricks Job — DBR 14.3]
                    ┌──────────────────────────────┐
                    │  databricks/notebooks/        │
                    │  01_bronze_ingest.py          │  Kafka → Bronze Delta (DBFS)
                    │  02_silver_transform.py       │  Clean + filter + deduplicate
                    │  03_gold_aggregate.py         │  Business aggregations
                    └──────────────────────────────┘
                               │
                               ▼
                    [dbt-duckdb] ──► [data/warehouse.duckdb]
                    (dbt/models/)                  │
                                                   ▼
                                         [Streamlit Dashboard]
                                         (src/interfaces/dashboard/app.py)
```

### Source code layers (`src/`)

```
src/
  domain/           # Pure logic — no framework imports
    entities/       # Listing (frozen dataclass)
    value_objects/  # Price, Mileage — validated on construction
    constants.py    # Single source of truth for all filter bounds (min/max price, year, mileage, EXCLUDED_MAKES)
    exceptions.py

  application/      # Use cases — depends only on domain interfaces
    ports/          # IListingProducer (Protocol) — the DDD anti-corruption boundary
    produce_listings.py
    stream_to_bronze.py
    transform_silver.py

  infrastructure/   # All I/O — Kafka, Spark/Delta, MinIO, secrets
    kafka/          # ListingProducer (satisfies IListingProducer structurally)
    spark/          # get_spark_session() factory — single canonical session config
    storage/        # MinIO client
    secrets/

  interfaces/
    cli/            # pipeline_cli.py — Click CLI
    dashboard/      # app.py — Streamlit
```

`kafka/` (top-level) — standalone Kafka config + producer used outside the DDD src tree (e.g., scripts).

`dags/` — Airflow DAG (`car_price_pipeline.py`) + custom operators.

### dbt model layers

| Folder | Materialisation | Purpose |
|--------|----------------|---------|
| `dbt/models/silver/` | view | Staging from Delta |
| `dbt/models/staging/` | view | Intermediate staging models |
| `dbt/models/marts/` | table | Business marts |
| `dbt/models/gold/` | table | Gold aggregations (mart_price_by_make, _state, _year, listings_summary) |

---

## Key invariants

- **Constants are the single source of truth.** Filter bounds (price, year, mileage) live in `src/domain/constants.py` for Python code and in `dbt_project.yml` `vars:` for SQL. Never hardcode these values elsewhere — they diverged silently before the refactor.

- **Application layer never imports from infrastructure directly.** `ProduceListings` takes `IListingProducer` (from `src/application/ports/`), not `ListingProducer`. Preserve this boundary.

- **`overwriteSchema` vs `mergeSchema`.** Delta writes that change schema must use `.option("overwriteSchema", "true")` with `mode("overwrite")`. `mergeSchema` is for append-only schema evolution. Using the wrong one causes `DELTA_FAILED_TO_MERGE_FIELDS` errors on existing Delta tables.

- **Column name: `mileage` not `odometer`.** The raw CSV has `odometer`; it is renamed to `mileage` at bronze ingest. All downstream code (domain, dbt) uses `mileage`. Never reintroduce `odometer` downstream.

- **dbt reads from MinIO Delta via `delta_scan()`.** Requires `httpfs` + `delta` DuckDB extensions and `s3_url_style: path` in `profiles.yml` for MinIO compatibility.

- **`get_spark_session()` is the only Spark session factory.** Located at `src/infrastructure/spark/session.py`. Do not create ad-hoc SparkSession instances elsewhere.

- **Dashboard table access must be allow-listed.** `src/interfaces/dashboard/app.py` validates table names against `_ALLOWED_TABLES` before interpolation. Do not bypass this.

---

## Adding a new Gold model (checklist)

1. Add aggregation to `databricks/notebooks/03_gold_aggregate.py`
2. Add staging model in `dbt/models/staging/stg_<name>.sql`
3. Add mart model in `dbt/models/gold/mart_<name>.sql` with CTE pattern (no `LIMIT` inside `WHERE IN`)
4. Add `.yml` file with `not_null`, `unique`, and `accepted_values` tests
5. If a new filter bound is introduced — add it to `src/domain/constants.py` **and** `dbt_project.yml vars:`

---

## Naming conventions

- dbt staging: `stg_<gold_table_name>.sql`
- dbt gold/marts: `mart_<business_concept>.sql`
- Databricks notebooks: `NN_<layer>_<verb>.py`
- Airflow tasks: `snake_case` verb-noun
- pytest markers: `spark` (needs JVM), `integration` (needs external services), `slow`

---

## Environment

Credentials are injected via env vars — see `infra/.env.example`. Required for non-local runs:
- `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_JOB_ID`
- `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_ENDPOINT`
- `KAFKA_BOOTSTRAP_SERVERS`
- `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY`

When running scripts on the host against Docker services, use published ports: MinIO→9000, Kafka→9092.
