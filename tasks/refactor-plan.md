# Refactor Plan — Data Engineering Pipeline

## Summary

The repo is a well-structured medallion-lakehouse pipeline (CSV → Kafka → Bronze → Silver → dbt Gold → Streamlit).
The domain layer is clean. The critical problems are a **split/inconsistent pipeline**, **duplicated Spark config**,
**schema column drift**, and several **correctness bugs** that silently produce wrong output.

---

## Findings by Severity

### P0 — Broken / Incorrect

**1. Dual pipeline with incompatible schemas**

Two completely separate pipelines exist side-by-side:

| | Script pipeline (`scripts/`) | Airflow pipeline (`dags/` + `src/`) |
|---|---|---|
| Bronze | `scripts/ingest_to_bronze.py` — reads CSV directly | `src/application/stream_to_bronze.py` — reads from Kafka |
| Silver | `scripts/transform_silver.py` — writes **local parquet** | `src/application/transform_silver.py` — writes Delta to MinIO |
| Column | keeps raw `odometer` | renames to `mileage` |
| dbt source | reads `../data/silver/listings/*.parquet` | never connected |

**Result**: dbt models only work with the script pipeline. The Airflow pipeline (the architected one)
never feeds dbt. The silver Delta on MinIO and the local parquet coexist with different schemas.

**2. `odometer` vs `mileage` column name drift**

- Raw CSV: `odometer`  
- `scripts/transform_silver.py` line 45: retains `odometer`  
- `src/application/transform_silver.py` line 34: filters on `mileage` (column renamed somewhere upstream that is never written)  
- `dbt/models/silver/stg_listings.sql` line 13: reads `odometer`, aliases as `mileage`  

The Airflow silver path filters `mileage` but bronze only has `odometer` — the filter silently drops
everything or crashes.

**3. delta-spark version mismatch**

- `pyproject.toml`: `delta-spark==3.0.0`
- `src/infrastructure/spark/session.py` jars: `delta-spark_2.12:3.2.0`
- `scripts/ingest_to_bronze.py` jars: `delta-spark_2.12:3.2.0`
- `docker-compose.yml` thriftserver: `delta-core_2.12:3.0.0`

Three different versions; the Python package and the JVM jar must match exactly.

**4. `dbt/models/sources.yml` declares `bronze.listings` but is never used**

`stg_listings.sql` reads from `read_parquet('../data/silver/listings/*.parquet')` — a hardcoded relative
path that only works when dbt is run from inside `dbt/`. The declared source is dead. dbt lineage is broken.

---

### P1 — Architecture Issues

**5. Spark session factory duplicated three times**

`get_spark()` / `get_spark_session()` exists in:
- `src/infrastructure/spark/session.py` (canonical)
- `scripts/ingest_to_bronze.py` (different jars, different defaults)
- `scripts/transform_silver.py` (same as above)
- `scripts/run_dbt.py` (adds `enableHiveSupport()`)

Scripts don't use the canonical factory. Config drift is inevitable.

**6. Business rules spread across four places**

Price/year/mileage bounds appear in:
- `src/domain/value_objects/price.py` (`>0`, `<=500_000`)
- `src/domain/entities/listing.py` `is_valid_year()` (`1900–2025`)
- `src/application/transform_silver.py` (`500–200_000`, `1990–2024`, `0–500_000`)
- `dbt/models/silver/int_listings_valid.sql` (same values, copy-pasted)

The domain says year ≤2025 but the Spark filter caps at 2024. These will silently diverge.

**7. Application layer directly imports infrastructure**

`src/application/produce_listings.py` imports `ListingProducer` from infrastructure — violating
the DDD rule that application depends only on domain interfaces.
No `IListingProducer` protocol exists.

**8. `stg_listings.sql` re-transforms already-clean data**

Silver Spark already lowercases/trims `make`/`model`/`state`. dbt's staging model applies the same
`LOWER(TRIM(...))` again — harmless but signals that the pipeline contract between layers is unclear.

---

### P2 — Security / Config

**9. Hardcoded credentials in 5 files**

`minioadmin`/`minioadmin` appears in:
- `src/infrastructure/spark/session.py`
- `src/infrastructure/storage/minio_client.py`
- `scripts/ingest_to_bronze.py`
- `scripts/transform_silver.py`
- `scripts/run_dbt.py`

Should be read from env vars with defaults only for local dev.

**10. SQL injection in dashboard**

`src/interfaces/dashboard/app.py` line 13:
```python
df = conn.execute(f"SELECT * FROM {table}").df()
```
`table` is hardcoded at call sites today, but the pattern allows injection. Should use a
whitelist or parameterized query.

**11. Kafka bootstrap server hardcoded in application layer**

`src/application/stream_to_bronze.py` line 29:
```python
.option("kafka.bootstrap.servers", "kafka:29092")
```
Infrastructure config in the application layer. Should be injected.

---

### P3 — Code Quality

**12. Exceptions swallowed silently in `_parse_batch`**

`src/application/produce_listings.py` line 37:
```python
except Exception:
    errors += 1
```
Error type and message are lost. Should at minimum log the exception.

**13. No structured logging — `print()` everywhere**

Scripts and application layer use raw `print()`. No log levels, no structured output,
no way to control verbosity.

**14. `run_dbt.py` imports PySpark at module level**

```python
from pyspark.sql import SparkSession  # top of file
```
Importing the module requires JVM startup even if only the dbt-run path is needed.
Should be inside `register_tables()`.

**15. `__pycache__` committed to git**

`src/` and `tests/` have committed `.pyc` files and `__pycache__/` dirs.

---

### P4 — Testing

**16. Integration test is actually a unit test**

`tests/integration/test_kafka_producer.py` patches `KafkaProducer` entirely — making it
equivalent to a unit test. The `tests/integration/` folder should contain tests that
connect to real services (or at least a Kafka testcontainer).

**17. No tests for `ProduceListings` or `TransformSilver`**

The two primary application use cases have zero test coverage.

---

### P5 — dbt

**18. Gold models have no column tests or documentation**

`stg_listings.yml` has tests on staging columns. The four gold mart models have no `.yml`
files with tests or column documentation.

**19. `mart_price_by_year` — top-10 filter is fragile**

```sql
WHERE make IN (
    SELECT make FROM {{ ref('mart_price_by_make') }}
    ORDER BY listing_count DESC LIMIT 10
)
```
The `LIMIT` inside `WHERE IN` is legal in DuckDB but unusual. Better expressed as a CTE or
a dbt variable so the top-N is configurable.

---

## Refactor Phases

### Phase 1 — Fix the broken pipeline (P0)

**Goal**: one coherent pipeline path, correct schemas, dbt working end-to-end.

1. **Rename `odometer` → `mileage` in bronze/Spark layer** so the column name is consistent
   all the way from ingestion to dbt. Update `stream_to_bronze.py` (KAFKA_SCHEMA),
   `scripts/ingest_to_bronze.py` (add rename), `scripts/transform_silver.py` (rename column).

2. **Delete the script-pipeline's local parquet write** — it's a dead end. Have
   `scripts/transform_silver.py` write Delta to MinIO silver (same as the Airflow path).

3. **Fix `dbt/models/silver/stg_listings.sql`** to read from the MinIO silver Delta table
   via `{{ source('silver', 'listings') }}` — matching the declared source. Update
   `sources.yml` to point to silver (not bronze).

4. **Align delta-spark to one version** — pick `3.2.0` throughout: `pyproject.toml`,
   all `spark.jars.packages` configs, docker-compose thriftserver.

### Phase 2 — Consolidate Spark config (P1 #5)

5. **Delete `get_spark()` from scripts** — make all scripts import and call
   `src.infrastructure.spark.session.get_spark_session()`.
   Add `local[*]` vs `spark://` toggle via `SPARK_MASTER_URL` env var (already done in
   the canonical factory — just wire it up).

### Phase 3 — Single source of truth for business rules (P1 #6)

6. **Extract filter constants** into `src/domain/constants.py` (or widen `Price`/`Mileage`
   validators to match the Spark filter). Expose them as Python constants that can also be
   referenced from a dbt variable.

   ```python
   # src/domain/constants.py
   MIN_PRICE = 500
   MAX_PRICE = 200_000
   MIN_YEAR  = 1990
   MAX_YEAR  = 2024
   MAX_MILEAGE = 500_000
   ```

   Update `transform_silver.py` and `int_listings_valid.sql` to reference these (via dbt vars
   for the SQL side).

### Phase 4 — Config & security (P2)

7. **Replace hardcoded credentials** — read `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`,
   `KAFKA_BOOTSTRAP_SERVERS` from env in all places. Keep `minioadmin` as default only.

8. **Fix dashboard SQL** — whitelist the four allowed table names; raise on unknown input.

9. **Inject Kafka servers into `StreamToBronze`** — accept as constructor param, default
   from env var.

### Phase 5 — Code quality (P3)

10. **Add structured logging** — swap `print()` for `logging.getLogger(__name__)` with
    appropriate levels. One-line change per file.

11. **Log exceptions in `_parse_batch`** — `logger.warning("Failed to parse row: %s", e)`.

12. **Move PySpark import in `run_dbt.py`** inside `register_tables()`.

13. **Add `__pycache__/` and `*.pyc` to `.gitignore`** and remove committed cache.

### Phase 6 — Domain interface for producer (P1 #7)

14. **Add `IListingProducer` protocol** in `src/domain/` (or `src/application/ports/`).
    `ProduceListings` depends on the protocol; `ListingProducer` implements it.

### Phase 7 — Tests (P4)

15. **Add unit tests for `ProduceListings`** — mock the producer protocol, verify batching logic.

16. **Add unit tests for `TransformSilver`** — use small in-memory Spark DataFrames.

17. **Move current integration test to `tests/unit/infrastructure/`**.

18. **Add a real integration test stub** with a `pytest.mark.integration` skip marker
    for CI until services are available.

### Phase 8 — dbt quality (P5)

19. **Add `.yml` docs and tests for all gold models** — at minimum `not_null` and
    `accepted_values` where applicable.

20. **Refactor `mart_price_by_year` top-10 filter** into a CTE.

---

## File Change Map

| File | Change |
|------|--------|
| `src/infrastructure/spark/session.py` | env-var credentials |
| `src/application/stream_to_bronze.py` | inject bootstrap servers; fix KAFKA_SCHEMA `odometer→mileage` |
| `src/application/transform_silver.py` | use constants |
| `src/application/produce_listings.py` | use protocol, log exceptions |
| `src/domain/constants.py` | **new** — filter bounds |
| `src/domain/ports/__init__.py` | **new** — `IListingProducer` protocol |
| `src/infrastructure/kafka/producer.py` | implement protocol, env-var bootstrap |
| `src/infrastructure/storage/minio_client.py` | env-var credentials |
| `scripts/ingest_to_bronze.py` | use canonical session factory, rename `odometer→mileage` |
| `scripts/transform_silver.py` | use canonical session, write Delta to MinIO |
| `scripts/run_dbt.py` | move PySpark import inside function |
| `dbt/models/silver/stg_listings.sql` | use `{{ source('silver','listings') }}` |
| `dbt/models/sources.yml` | point to silver |
| `dbt/models/silver/int_listings_valid.sql` | use dbt vars for bounds |
| `dbt/models/gold/*.yml` | **new** — column tests + docs |
| `dbt/models/gold/mart_price_by_year.sql` | CTE for top-10 |
| `src/interfaces/dashboard/app.py` | whitelist table names |
| `tests/unit/application/` | **new** — ProduceListings, TransformSilver tests |
| `.gitignore` | add `__pycache__/`, `*.pyc` |
| `pyproject.toml` | `delta-spark==3.2.0` |

---

## Out of Scope (this refactor)

- Switching Airflow executor from SequentialExecutor to LocalExecutor
- Adding Kafka Schema Registry / Avro schemas
- Real secrets management (Vault, AWS Secrets Manager)
- Spark unit testing framework (e.g., `chispa`)
- Adding a Gold Delta → DuckDB sync step (currently manual)
