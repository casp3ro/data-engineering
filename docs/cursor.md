# Car Price Pipeline — Cursor Instructions

## Rules

- Python 3.11
- Use `uv` for all package management, never `pip` directly
- All code must have type hints
- Pydantic v2 for all data models
- Domain layer has zero I/O — no spark, no kafka, no file reads
- Every class has a single responsibility
- No business logic in infrastructure layer
- Follow the exact directory structure below

---

## Directory Structure

Create this exact structure before writing any code:

```
car-price-pipeline/
├── docker-compose.yml
├── pyproject.toml
├── Dockerfile.dashboard
├── scripts/
│   └── setup_minio.py
├── src/
│   ├── __init__.py
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── entities/
│   │   │   ├── __init__.py
│   │   │   └── listing.py
│   │   ├── value_objects/
│   │   │   ├── __init__.py
│   │   │   ├── price.py
│   │   │   └── mileage.py
│   │   └── exceptions.py
│   ├── application/
│   │   ├── __init__.py
│   │   ├── produce_listings.py
│   │   ├── stream_to_bronze.py
│   │   └── transform_silver.py
│   ├── infrastructure/
│   │   ├── __init__.py
│   │   ├── kafka/
│   │   │   ├── __init__.py
│   │   │   └── producer.py
│   │   ├── spark/
│   │   │   ├── __init__.py
│   │   │   ├── session.py
│   │   │   └── delta_writer.py
│   │   └── storage/
│   │       ├── __init__.py
│   │       └── minio_client.py
│   └── interfaces/
│       ├── __init__.py
│       ├── cli/
│       │   ├── __init__.py
│       │   └── pipeline_cli.py
│       └── dashboard/
│           ├── __init__.py
│           └── app.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── clean_string.sql
│   ├── models/
│   │   ├── sources.yml
│   │   ├── silver/
│   │   │   ├── stg_listings.sql
│   │   │   ├── stg_listings.yml
│   │   │   └── int_listings_valid.sql
│   │   └── gold/
│   │       ├── mart_price_by_make.sql
│   │       ├── mart_price_by_year.sql
│   │       ├── mart_price_by_state.sql
│   │       └── mart_listings_summary.sql
│   └── tests/
│       └── assert_price_positive.sql
├── dags/
│   └── car_price_pipeline.py
├── data/
│   └── raw/
└── tests/
    ├── __init__.py
    ├── unit/
    │   ├── __init__.py
    │   └── domain/
    │       ├── __init__.py
    │       ├── test_price.py
    │       ├── test_mileage.py
    │       └── test_listing.py
    └── integration/
        ├── __init__.py
        └── test_kafka_producer.py
```

---

## Task 1 — pyproject.toml

Create `pyproject.toml`:

```toml
[project]
name = "car-price-pipeline"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "pyspark==3.5.0",
    "delta-spark==3.0.0",
    "kafka-python==2.0.2",
    "dbt-spark[PyHive]==1.7.0",
    "polars>=0.20.0",
    "pydantic>=2.0.0",
    "streamlit>=1.30.0",
    "plotly>=5.0.0",
    "minio>=7.0.0",
    "click>=8.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-mock>=3.0.0",
    "ruff>=0.3.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]

[tool.ruff]
line-length = 88
```

---

## Task 2 — docker-compose.yml

Create `docker-compose.yml` with these exact services:

**MinIO** (S3-compatible storage)

- image: `minio/minio:latest`
- ports: 9000 (API), 9001 (Console)
- env: MINIO_ROOT_USER=minioadmin, MINIO_ROOT_PASSWORD=minioadmin
- command: `server /data --console-address ":9001"`
- healthcheck on `http://localhost:9000/minio/health/live`

**Zookeeper**

- image: `confluentinc/cp-zookeeper:7.5.0`
- env: ZOOKEEPER_CLIENT_PORT=2181

**Kafka**

- image: `confluentinc/cp-kafka:7.5.0`
- port: 9092
- depends_on: zookeeper
- env: KAFKA_BROKER_ID=1, KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1, KAFKA_AUTO_CREATE_TOPICS_ENABLE=true
- KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092`

**Kafka UI**

- image: `provectuslabs/kafka-ui:latest`
- port: 8085→8080
- KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: `kafka:29092`

**Spark Master**

- image: `bitnami/spark:3.5.0`
- ports: 8081→8080, 7077
- env: SPARK_MODE=master
- volumes: `./src:/opt/bitnami/spark/src`, `./data:/opt/bitnami/spark/data`

**Spark Worker**

- image: `bitnami/spark:3.5.0`
- env: SPARK_MODE=worker, SPARK_MASTER_URL=spark://spark-master:7077, SPARK_WORKER_MEMORY=2G, SPARK_WORKER_CORES=2
- depends_on: spark-master

**Airflow**

- image: `apache/airflow:2.8.0-python3.11`
- port: 8080
- depends_on: kafka, spark-master, minio
- volumes: dags, src, dbt, data
- env: AIRFLOW**CORE**LOAD_EXAMPLES=false, LocalExecutor

**Dashboard**

- build from Dockerfile.dashboard
- port: 8501
- volumes: src/interfaces/dashboard, data

All services on network named `pipeline`. Volumes: `minio_data`, `airflow_db`.

---

## Task 3 — Domain Layer

### File: `src/domain/value_objects/price.py`

```python
from pydantic import BaseModel, field_validator


class Price(BaseModel):
    amount: float

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"Price must be positive, got {v}")
        if v > 500_000:
            raise ValueError(f"Price unrealistically high: {v}")
        return round(v, 2)

    def __str__(self) -> str:
        return f"USD {self.amount:,.2f}"
```

### File: `src/domain/value_objects/mileage.py`

```python
from pydantic import BaseModel, field_validator


class Mileage(BaseModel):
    value: int

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"Mileage cannot be negative: {v}")
        if v > 1_000_000:
            raise ValueError(f"Mileage unrealistically high: {v}")
        return v
```

### File: `src/domain/entities/listing.py`

```python
from dataclasses import dataclass
from typing import Optional

from src.domain.value_objects.mileage import Mileage
from src.domain.value_objects.price import Price


@dataclass(frozen=True)
class Listing:
    id: str
    make: str
    model: str
    year: int
    price: Price
    mileage: Mileage
    state: str
    condition: Optional[str] = None

    def is_valid_year(self) -> bool:
        return 1900 <= self.year <= 2025

    def price_per_mile(self) -> Optional[float]:
        if self.mileage.value == 0:
            return None
        return round(self.price.amount / self.mileage.value, 4)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "make": self.make,
            "model": self.model,
            "year": self.year,
            "price": self.price.amount,
            "mileage": self.mileage.value,
            "state": self.state,
            "condition": self.condition,
        }
```

### File: `src/domain/exceptions.py`

```python
class DomainException(Exception):
    pass

class InvalidPriceError(DomainException):
    pass

class InvalidMileageError(DomainException):
    pass

class InvalidListingError(DomainException):
    pass
```

---

## Task 4 — Infrastructure: Spark

### File: `src/infrastructure/spark/session.py`

```python
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "CarPricePipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://localhost:7077")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )
```

### File: `src/infrastructure/spark/delta_writer.py`

```python
from delta.tables import DeltaTable
from pyspark.sql import DataFrame


class DeltaWriter:
    BASE = "s3a://"

    def write_bronze(self, df: DataFrame, table: str) -> None:
        path = f"{self.BASE}bronze/{table}"
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(path)
        )

    def upsert_silver(
        self,
        df: DataFrame,
        table: str,
        merge_key: str = "id",
    ) -> None:
        path = f"{self.BASE}silver/{table}"
        spark = df.sparkSession

        if DeltaTable.isDeltaTable(spark, path):
            target = DeltaTable.forPath(spark, path)
            (
                target.alias("target")
                .merge(
                    df.alias("source"),
                    f"target.{merge_key} = source.{merge_key}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            df.write.format("delta").mode("overwrite").save(path)
```

---

## Task 5 — Infrastructure: Kafka

### File: `src/infrastructure/kafka/producer.py`

```python
import json
from typing import Iterator

from kafka import KafkaProducer


TOPIC = "car_listings_raw"


class ListingProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )

    def send(self, listing_dict: dict) -> None:
        self._producer.send(
            topic=TOPIC,
            key=listing_dict["id"],
            value=listing_dict,
        )

    def send_batch(self, listings: Iterator[dict]) -> int:
        count = 0
        for listing in listings:
            self.send(listing)
            count += 1
        self._producer.flush()
        return count

    def close(self) -> None:
        self._producer.close()
```

---

## Task 6 — Infrastructure: MinIO

### File: `src/infrastructure/storage/minio_client.py`

```python
from minio import Minio


BUCKETS = ["bronze", "silver", "gold"]


class MinioClient:
    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
    ) -> None:
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

    def setup_buckets(self) -> None:
        for bucket in BUCKETS:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
                print(f"Created bucket: {bucket}")
            else:
                print(f"Bucket exists: {bucket}")
```

### File: `scripts/setup_minio.py`

```python
from src.infrastructure.storage.minio_client import MinioClient

if __name__ == "__main__":
    client = MinioClient()
    client.setup_buckets()
    print("MinIO setup complete.")
```

---

## Task 7 — Application: Produce Listings

### File: `src/application/produce_listings.py`

```python
from pathlib import Path

import polars as pl

from src.domain.entities.listing import Listing
from src.domain.value_objects.mileage import Mileage
from src.domain.value_objects.price import Price
from src.infrastructure.kafka.producer import ListingProducer

BATCH_SIZE = 1_000


class ProduceListings:
    def __init__(self, producer: ListingProducer) -> None:
        self._producer = producer

    def execute(self, csv_path: Path) -> dict:
        total, failed = 0, 0
        df = pl.read_csv(csv_path, infer_schema_length=10_000)

        for i in range(0, len(df), BATCH_SIZE):
            batch = df.slice(i, BATCH_SIZE)
            valid, errors = self._parse_batch(batch)
            self._producer.send_batch(iter(valid))
            total += len(valid)
            failed += errors

        return {"produced": total, "failed": failed}

    def _parse_batch(self, batch: pl.DataFrame) -> tuple[list[dict], int]:
        valid: list[dict] = []
        errors = 0
        for row in batch.iter_rows(named=True):
            try:
                listing = self._parse_row(row)
                valid.append(listing.to_dict())
            except Exception:
                errors += 1
        return valid, errors

    def _parse_row(self, row: dict) -> Listing:
        return Listing(
            id=str(row["id"]),
            make=str(row.get("manufacturer", "")).lower().strip(),
            model=str(row.get("model", "")).lower().strip(),
            year=int(row["year"]),
            price=Price(amount=float(row["price"])),
            mileage=Mileage(value=int(row.get("odometer", 0))),
            state=str(row.get("state", "")),
            condition=row.get("condition"),
        )
```

---

## Task 8 — Application: Stream to Bronze

### File: `src/application/stream_to_bronze.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

KAFKA_SCHEMA = StructType([
    StructField("id",        StringType(),  True),
    StructField("make",      StringType(),  True),
    StructField("model",     StringType(),  True),
    StructField("year",      IntegerType(), True),
    StructField("price",     DoubleType(),  True),
    StructField("mileage",   IntegerType(), True),
    StructField("state",     StringType(),  True),
    StructField("condition", StringType(),  True),
])

BRONZE_PATH = "s3a://bronze/listings"
CHECKPOINT  = "s3a://bronze/_checkpoints/listings"


class StreamToBronze:
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def run(self) -> None:
        raw = (
            self._spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
            .option("subscribe", "car_listings_raw")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = (
            raw
            .select(F.from_json(F.col("value").cast("string"), KAFKA_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("ingested_at", F.current_timestamp())
        )

        query = (
            parsed.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT)
            .option("mergeSchema", "true")
            .start(BRONZE_PATH)
        )

        query.awaitTermination()
```

---

## Task 9 — Application: Transform Silver

### File: `src/application/transform_silver.py`

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.infrastructure.spark.delta_writer import DeltaWriter

BRONZE_PATH = "s3a://bronze/listings"


class TransformSilver:
    def __init__(self, spark: SparkSession, writer: DeltaWriter) -> None:
        self._spark  = spark
        self._writer = writer

    def execute(self) -> dict:
        bronze = self._spark.read.format("delta").load(BRONZE_PATH)
        silver = self._clean(bronze)
        silver = self._filter(silver)
        self._writer.upsert_silver(silver, "listings")
        return {"silver_count": silver.count()}

    def _clean(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("make",      F.lower(F.trim(F.col("make"))))
            .withColumn("model",     F.lower(F.trim(F.col("model"))))
            .withColumn("state",     F.upper(F.trim(F.col("state"))))
            .withColumn("log_price", F.log1p(F.col("price")))
            .dropDuplicates(["id"])
        )

    def _filter(self, df: DataFrame) -> DataFrame:
        return df.filter(
            F.col("price").between(500, 200_000)
            & F.col("year").between(1990, 2024)
            & F.col("mileage").between(0, 500_000)
            & F.col("make").isNotNull()
            & (F.col("make") != "")
        )
```

---

## Task 10 — dbt Models

### File: `dbt/dbt_project.yml`

```yaml
name: car_price
version: "1.0.0"
config-version: 2
profile: car_price
model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

models:
  car_price:
    silver:
      +materialized: view
      +file_format: delta
    gold:
      +materialized: table
      +file_format: delta
      +location_root: s3a://gold
```

### File: `dbt/profiles.yml`

```yaml
car_price:
  target: local
  outputs:
    local:
      type: spark
      method: thrift
      host: localhost
      port: 10000
      schema: gold
      threads: 4
```

### File: `dbt/macros/clean_string.sql`

```sql
{% macro clean_string(column_name) %}
    LOWER(TRIM(REGEXP_REPLACE({{ column_name }}, '[^a-zA-Z0-9 ]', '', 'g')))
{% endmacro %}
```

### File: `dbt/models/silver/stg_listings.sql`

```sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'listings') }}
),
cleaned AS (
    SELECT
        id,
        {{ clean_string('make') }}  AS make,
        {{ clean_string('model') }} AS model,
        year,
        price,
        mileage,
        UPPER(state)                AS state,
        condition,
        ingested_at
    FROM source
    WHERE price IS NOT NULL
      AND year  IS NOT NULL
      AND make  IS NOT NULL
)
SELECT * FROM cleaned
```

### File: `dbt/models/silver/int_listings_valid.sql`

```sql
{{ config(materialized='table', file_format='delta') }}

SELECT *
FROM {{ ref('stg_listings') }}
WHERE
    price   BETWEEN 500   AND 200000
    AND year BETWEEN 1990  AND 2024
    AND mileage BETWEEN 0  AND 500000
    AND make NOT IN ('', 'unknown')
    AND state IS NOT NULL
```

### File: `dbt/models/gold/mart_price_by_make.sql`

```sql
{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    make,
    COUNT(*)                         AS listing_count,
    ROUND(PERCENTILE(price, 0.5), 0) AS median_price,
    ROUND(AVG(price), 0)             AS avg_price,
    ROUND(STDDEV(price), 0)          AS price_stddev,
    ROUND(MIN(price), 0)             AS min_price,
    ROUND(MAX(price), 0)             AS max_price
FROM {{ ref('int_listings_valid') }}
GROUP BY make
HAVING COUNT(*) >= 50
ORDER BY median_price DESC
```

### File: `dbt/models/gold/mart_price_by_year.sql`

```sql
{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    make,
    year,
    COUNT(*)                         AS listing_count,
    ROUND(PERCENTILE(price, 0.5), 0) AS median_price
FROM {{ ref('int_listings_valid') }}
WHERE make IN (
    SELECT make FROM {{ ref('mart_price_by_make') }}
    ORDER BY listing_count DESC LIMIT 10
)
GROUP BY make, year
ORDER BY make, year
```

### File: `dbt/models/gold/mart_price_by_state.sql`

```sql
{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    state,
    COUNT(*)                         AS listing_count,
    ROUND(PERCENTILE(price, 0.5), 0) AS median_price,
    ROUND(AVG(price), 0)             AS avg_price
FROM {{ ref('int_listings_valid') }}
GROUP BY state
ORDER BY median_price DESC
```

### File: `dbt/models/gold/mart_listings_summary.sql`

```sql
{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    COUNT(*)                         AS total_listings,
    COUNT(DISTINCT make)             AS unique_makes,
    COUNT(DISTINCT state)            AS states_covered,
    ROUND(PERCENTILE(price, 0.5), 0) AS overall_median_price,
    ROUND(AVG(price), 0)             AS overall_avg_price,
    MIN(year)                        AS oldest_year,
    MAX(year)                        AS newest_year
FROM {{ ref('int_listings_valid') }}
```

---

## Task 11 — Airflow DAG

### File: `dags/car_price_pipeline.py`

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "kacper",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
)


def _produce() -> None:
    from pathlib import Path
    from src.application.produce_listings import ProduceListings
    from src.infrastructure.kafka.producer import ListingProducer

    producer = ListingProducer(bootstrap_servers="kafka:29092")
    result = ProduceListings(producer).execute(
        Path("/opt/airflow/data/raw/vehicles.csv")
    )
    print(f"Kafka produce: {result}")
    producer.close()


with DAG(
    dag_id="car_price_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "de", "spark", "kafka"],
) as dag:

    kafka_produce = PythonOperator(
        task_id="kafka_produce",
        python_callable=_produce,
    )

    spark_bronze = SparkSubmitOperator(
        task_id="spark_bronze",
        application="/opt/airflow/src/application/stream_to_bronze.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
    )

    spark_silver = SparkSubmitOperator(
        task_id="spark_silver",
        application="/opt/airflow/src/application/transform_silver.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    kafka_produce >> spark_bronze >> spark_silver >> dbt_run >> dbt_test
```

---

## Task 12 — Dashboard

### File: `src/interfaces/dashboard/app.py`

```python
import pandas as pd
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="Car Price Pipeline", page_icon="🚗", layout="wide")


@st.cache_resource
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Dashboard")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


@st.cache_data
def load(path: str) -> pd.DataFrame:
    return get_spark().read.format("delta").load(path).toPandas()


st.title("🚗 Car Price Pipeline")
st.caption("Spark · Delta Lake · Kafka · dbt · Airflow · MinIO")

summary = load("s3a://gold/mart_listings_summary")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Listings",  f"{summary['total_listings'][0]:,}")
c2.metric("Unique Makes",    summary['unique_makes'][0])
c3.metric("States Covered",  summary['states_covered'][0])
c4.metric("Median Price",   f"${summary['overall_median_price'][0]:,}")

st.divider()

st.subheader("Median Price by Make — Top 20")
makes = load("s3a://gold/mart_price_by_make")
st.plotly_chart(
    px.bar(makes.head(20), x="make", y="median_price", color="listing_count",
           color_continuous_scale="Blues"),
    use_container_width=True,
)

st.subheader("Price Depreciation by Year")
depr = load("s3a://gold/mart_price_by_year")
st.plotly_chart(
    px.line(depr, x="year", y="median_price", color="make"),
    use_container_width=True,
)

st.subheader("Median Price by State")
geo = load("s3a://gold/mart_price_by_state")
st.plotly_chart(
    px.choropleth(geo, locations="state", locationmode="USA-states",
                  color="median_price", scope="usa", color_continuous_scale="Viridis"),
    use_container_width=True,
)
```

---

## Task 13 — Unit Tests

### File: `tests/unit/domain/test_price.py`

```python
import pytest
from src.domain.value_objects.price import Price


@pytest.mark.parametrize("amount", [500.0, 12_000.0, 199_999.0])
def test_valid_prices(amount: float) -> None:
    assert Price(amount=amount).amount == amount


@pytest.mark.parametrize("amount", [0.0, -100.0, 600_000.0])
def test_invalid_prices(amount: float) -> None:
    with pytest.raises(Exception):
        Price(amount=amount)
```

### File: `tests/unit/domain/test_mileage.py`

```python
import pytest
from src.domain.value_objects.mileage import Mileage


@pytest.mark.parametrize("value", [0, 50_000, 499_999])
def test_valid_mileage(value: int) -> None:
    assert Mileage(value=value).value == value


@pytest.mark.parametrize("value", [-1, 1_000_001])
def test_invalid_mileage(value: int) -> None:
    with pytest.raises(Exception):
        Mileage(value=value)
```

### File: `tests/unit/domain/test_listing.py`

```python
from src.domain.entities.listing import Listing
from src.domain.value_objects.mileage import Mileage
from src.domain.value_objects.price import Price


def make_listing(**kwargs) -> Listing:
    defaults = dict(
        id="test-1", make="toyota", model="camry", year=2018,
        price=Price(amount=12_000.0), mileage=Mileage(value=80_000), state="ca",
    )
    return Listing(**{**defaults, **kwargs})


def test_valid_year() -> None:
    assert make_listing(year=2018).is_valid_year()

def test_invalid_year() -> None:
    assert not make_listing(year=1800).is_valid_year()

def test_price_per_mile() -> None:
    assert make_listing(
        price=Price(amount=10_000.0), mileage=Mileage(value=100_000)
    ).price_per_mile() == 0.1

def test_zero_mileage_returns_none() -> None:
    assert make_listing(mileage=Mileage(value=0)).price_per_mile() is None

def test_to_dict_has_all_keys() -> None:
    keys = make_listing().to_dict().keys()
    assert set(keys) == {"id", "make", "model", "year", "price", "mileage", "state", "condition"}
```

### File: `tests/integration/test_kafka_producer.py`

```python
from unittest.mock import patch
from src.infrastructure.kafka.producer import ListingProducer


def test_send_batch_returns_correct_count() -> None:
    with patch("src.infrastructure.kafka.producer.KafkaProducer"):
        producer = ListingProducer()
        rows = [{"id": str(i), "make": "toyota"} for i in range(10)]
        assert producer.send_batch(iter(rows)) == 10


def test_send_calls_kafka_send() -> None:
    with patch("src.infrastructure.kafka.producer.KafkaProducer") as mock_kafka:
        producer = ListingProducer()
        producer.send({"id": "abc", "make": "ford"})
        mock_kafka.return_value.send.assert_called_once()
```

---

## Run Commands

```bash
# 1. Install dependencies
uv sync --extra dev

# 2. Start all Docker services
docker compose up -d

# 3. Wait ~60s, then setup MinIO buckets
uv run python scripts/setup_minio.py

# 4. Run unit tests
uv run pytest tests/unit -v

# 5. Run dbt
cd dbt && dbt run --profiles-dir . && dbt test --profiles-dir .

# 6. Launch dashboard
uv run streamlit run src/interfaces/dashboard/app.py
```

---

## Service URLs

| Service   | URL                   | Credentials             |
| --------- | --------------------- | ----------------------- |
| Airflow   | http://localhost:8080 | admin / admin           |
| Spark UI  | http://localhost:8081 | —                       |
| MinIO     | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI  | http://localhost:8085 | —                       |
| Dashboard | http://localhost:8501 | —                       |
