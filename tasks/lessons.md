# Lessons — Data Engineering Pipeline Refactor

## Session: 2026-05-13/14 — 8-phase refactor

---

### [2026-05-13] Phase 1 — Unified pipeline & schema consistency

**What happened:** Two parallel pipelines existed (`scripts/` and `src/` + Airflow) that never
converged. `scripts/transform_silver.py` wrote to local parquet; the Airflow path wrote to
MinIO Delta. dbt read from local parquet via hardcoded path. Column `odometer` existed in
the raw CSV and scripts, but `mileage` was used in the Kafka/application layer — silent drift.

**Correct approach:**
- Single silver destination: `s3a://silver/listings` (Delta on MinIO) for both paths.
- Rename `odometer → mileage` immediately after CSV load in `ingest_to_bronze.py`.
- dbt reads from MinIO Delta directly via `delta_scan('s3://silver/listings')` with DuckDB
  `httpfs` + `delta` extensions configured in `profiles.yml`.
- `dbt/models/sources.yml` must declare silver, not bronze.
- `delta-spark` version must match across `pyproject.toml`, `spark.jars.packages`, and docker-compose.
- `scripts/run_dbt.py` had a dead `register_tables()` that registered Spark Hive catalog entries
  dbt-duckdb could never see — removed entirely.

---

### [2026-05-13] Phase 2 — Spark session factory

**What happened:** `get_spark()` was copy-pasted into `scripts/ingest_to_bronze.py` and
`scripts/transform_silver.py` with slight differences (memory config, no Kafka jars, different
master default). Config drift was inevitable.

**Correct approach:**
- One canonical `get_spark_session()` in `src/infrastructure/spark/session.py` with
  `driver_memory` and `shuffle_partitions` parameters.
- Default master changed to `local[*]` (works without Docker; Docker overrides via
  `SPARK_MASTER_URL` env var already set in docker-compose).
- Scripts import from the canonical factory and delete their own `get_spark()`.

---

### [2026-05-13] Phase 3 — Business rule constants

**What happened:** Filter bounds (price 500–200k, year 1990–2024, mileage 0–500k) were
copy-pasted in domain value objects, Spark application layer, Spark scripts, and dbt SQL.
They diverged silently (domain said year ≤ 2025, filter said ≤ 2024). The `"unknown"` make
exclusion existed in dbt but not in Spark.

**Correct approach:**
- `src/domain/constants.py` — single Python source of truth for all filter bounds + `EXCLUDED_MAKES`.
- Both Spark paths import from constants.
- `dbt_project.yml` declares matching `vars:` block. SQL models use `{{ var('...') }}`.
- The Python ↔ dbt boundary is two clearly named files, not scattered magic numbers.

---

### [2026-05-13] Phase 4 — Config & security

**What happened:** `minioadmin`/`minioadmin` hardcoded in 5 files.
`kafka:29092` hardcoded in application layer (`stream_to_bronze.py`).
Dashboard had SQL injection pattern: `f"SELECT * FROM {table}"`.

**Correct approach:**
- Infrastructure classes read credentials from env vars with local defaults
  (`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_ENDPOINT`, `KAFKA_BOOTSTRAP_SERVERS`).
- `StreamToBronze` accepts `bootstrap_servers` as constructor param; falls back to env var.
- Dashboard: `_ALLOWED_TABLES` frozenset, validate before interpolating.
- `dbt/profiles.yml`: use `{{ env_var('MINIO_ACCESS_KEY', 'minioadmin') }}`.

---

### [2026-05-13] Phase 5 — Logging & code quality

**What happened:** All files used `print()`. `_parse_batch` swallowed exceptions silently
(`except Exception: errors += 1` — no message, no row id). `__pycache__/` directories were
tracked in git (`.gitignore` already had the rule, files were just previously committed).

**Correct approach:**
- `logger = logging.getLogger(__name__)` in every module.
- Scripts call `logging.basicConfig(level=logging.INFO, ...)` inside `main()` only.
- `except Exception as e:` + `logger.warning("Failed to parse row id=%s: %s", ..., e)`.
- `git rm -r --cached` to untrack `__pycache__/` directories.

---

### [2026-05-13] Phase 6 — DDD layer boundary

**What happened:** `src/application/produce_listings.py` imported `ListingProducer` directly
from `src/infrastructure/kafka/producer.py` — application layer depending on infrastructure.

**Correct approach:**
- `src/application/ports/listing_producer.py` — `IListingProducer(Protocol)` with single
  method `send_batch(listings: Iterator[dict]) -> int`.
- `ProduceListings.__init__` takes `IListingProducer`, not `ListingProducer`.
- `ListingProducer` satisfies the protocol implicitly (structural subtyping) — no changes to
  infrastructure or callers needed.
- Mark `@runtime_checkable` to allow `isinstance()` checks in tests.

---

### [2026-05-13] Phase 7 — Tests

**What happened:** No tests for the two main use cases (`ProduceListings`, `TransformSilver`).
`tests/integration/test_kafka_producer.py` was patching `KafkaProducer` — unit test behaviour
living in the wrong folder.

**Correct approach:**
- `ProduceListings` unit tests: use a `_FakeProducer` stub that satisfies `IListingProducer`.
  Test with tmp_path CSV files. Cover: valid rows, failed rows, empty CSV, batch chunking.
- `TransformSilver` Spark tests: minimal `local[1]` session without `spark.jars.packages`
  (no Maven downloads, starts in ~3s). Test `_clean` and `_filter` directly.
  Mark `@pytest.mark.spark` so they run as a separate CI step.
- Move patched Kafka tests to `tests/unit/infrastructure/`.
- Real integration stub in `tests/integration/` skips unless `KAFKA_BOOTSTRAP_SERVERS` is set.
- Register markers in `pyproject.toml`; update CI to run fast + spark as separate steps.

---

### [2026-05-13] Phase 8 — dbt quality

**What happened:** Gold mart models had zero column tests or documentation.
`mart_price_by_year` used `WHERE make IN (SELECT ... ORDER BY ... LIMIT 10)` — a subquery
with `LIMIT` inside `WHERE IN` (legal in DuckDB but fragile and not obvious).

**Correct approach:**
- Four `.yml` files for gold models with `not_null`, `unique`, `accepted_values` tests.
- `mart_price_by_state` gets `accepted_values` for all 50 US state codes — strongest guard
  against upstream state normalisation regressions reaching gold.
- `mart_price_by_year`: CTE (`top_makes AS (... LIMIT {{ var('top_makes_limit', 10) }})`)
  + `INNER JOIN` replaces the subquery. The `10` becomes `top_makes_limit` dbt var.

---

### [2026-05-14] Pipeline validation — overwriteSchema bug

**What happened:** First full `docker compose up -d && python scripts/run_all.py` run after
refactor failed at the silver write step:
`DELTA_FAILED_TO_MERGE_FIELDS: Failed to merge fields 'odometer' and 'odometer'`

The existing silver Delta table on MinIO was written before the refactor (with `odometer` column).
`scripts/transform_silver.py` was using `.option("mergeSchema", "true")` with `mode("overwrite")`.
`mergeSchema` is for **append** operations — it adds new columns but never removes old ones.
For a full schema-changing overwrite, the correct option is `overwriteSchema`.

**Correct approach:**
- Use `.option("overwriteSchema", "true")` with `mode("overwrite")` in Delta writes where
  the schema is intentionally changing.
- Use `.option("mergeSchema", "true")` only with `mode("append")` to evolve schema additively.
- After renaming a column in an existing pipeline, stale Delta tables in MinIO must be deleted
  before re-running. Clean with:
  ```
  docker run --rm --network data-engineering_pipeline \
    -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 \
    minio/mc:latest sh -c "mc rm --recursive --force local/silver/listings/"
  ```

---

### General patterns observed in this codebase

- Scripts that run on the host connect to Docker services via published ports (MinIO:9000,
  Kafka:9092, Spark:7077). Set env vars if Docker network differs.
- Maven jar cache corruption (`~/.ivy2`, `~/.m2`) causes cryptic Spark startup failures.
  Fix: `rm -rf ~/.ivy2/cache/<group>` for the affected artifact.
- `dbt-duckdb` profiles.yml supports `env_var()` Jinja for credential injection.
- `delta_scan('s3://...')` in DuckDB requires both `httpfs` and `delta` extensions loaded,
  plus `s3_url_style: path` for MinIO compatibility.
