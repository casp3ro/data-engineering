# Car Price Pipeline — Full Production Stack (Local / Docker)

> End-to-end Data Engineering portfolio project | Zero cloud dependencies | ~16h weekend

---

## Stack

| Technologia                | Rola                           | Odpowiednik w produkcji |
| -------------------------- | ------------------------------ | ----------------------- |
| Apache Spark (PySpark)     | Distributed processing         | Databricks / EMR        |
| Delta Lake                 | ACID storage layer             | Databricks Delta        |
| dbt-spark                  | Transformacje SQL              | dbt na Databricks       |
| Apache Kafka + Zookeeper   | Event streaming                | Confluent / MSK         |
| Apache Airflow             | Orchestration                  | Cloud Composer / MWAA   |
| Spark Structured Streaming | Real-time processing           | Databricks Streaming    |
| MinIO                      | Object storage (S3-compatible) | GCP Cloud Storage / S3  |
| Parquet + Avro             | File formats                   | Produkcyjny standard    |
| Streamlit                  | Dashboard                      | Looker / Superset       |
| Pytest                     | Testing                        | CI/CD pipeline          |
| Docker Compose             | Całe środowisko lokalnie       | Kubernetes              |

---

## Architecture

```
Raw CSV
    │
    ▼
Kafka Topic (car_listings_raw)
    │
    ▼
Spark Structured Streaming
    │
    ▼
MinIO / Bronze Layer (Parquet)         ← raw, immutable
    │
    ▼
Spark + Delta Lake / Silver Layer      ← cleaned, validated
    │
    ▼
dbt-spark / Gold Layer (Delta tables)  ← aggregated marts
    │
    ▼
Streamlit Dashboard
    │
Airflow DAG orchestrates everything
```

```
project/
├── docker-compose.yml
├── pyproject.toml
│
├── src/
│   ├── domain/
│   │   ├── entities/
│   │   │   └── listing.py
│   │   └── value_objects/
│   │       ├── price.py
│   │       └── mileage.py
│   │
│   ├── application/
│   │   ├── produce_listings.py      # Kafka producer
│   │   ├── stream_to_bronze.py      # Spark Structured Streaming
│   │   ├── transform_silver.py      # Spark batch → Silver Delta
│   │   └── compute_gold.py          # trigger dbt run
│   │
│   ├── infrastructure/
│   │   ├── kafka/
│   │   │   ├── producer.py
│   │   │   └── consumer.py
│   │   ├── spark/
│   │   │   ├── session.py           # SparkSession factory
│   │   │   └── delta_writer.py      # Delta Lake write helper
│   │   └── storage/
│   │       └── minio_client.py
│   │
│   └── interfaces/
│       ├── cli/
│       │   └── pipeline_cli.py
│       └── dashboard/
│           └── app.py
│
├── dbt/
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_listings.sql
│   │   │   ├── stg_listings.yml
│   │   │   └── int_listings_valid.sql
│   │   └── gold/
│   │       ├── mart_price_by_make.sql
│   │       ├── mart_price_by_year.sql
│   │       ├── mart_price_by_state.sql
│   │       └── mart_listings_summary.sql
│   ├── macros/
│   │   └── clean_string.sql
│   └── dbt_project.yml
│
├── dags/
│   └── car_price_pipeline.py
│
└── tests/
    ├── unit/
    │   └── domain/
    │       ├── test_price.py
    │       ├── test_mileage.py
    │       └── test_listing.py
    └── integration/
        ├── test_kafka_producer.py
        └── test_spark_delta.py
```

---

## Docker Compose — pełne środowisko

```yaml
# docker-compose.yml
version: "3.8"

networks:
  pipeline:
    driver: bridge

volumes:
  minio_data:
  airflow_db:

services:
  # ── Object Storage (S3-compatible) ─────────────────────────────────────────
  minio:
    image: minio/minio:latest
    container_name: minio
    networks: [pipeline]
    ports:
      - "9000:9000" # API
      - "9001:9001" # Console UI → http://localhost:9001
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ── Kafka + Zookeeper ───────────────────────────────────────────────────────
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    networks: [pipeline]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    networks: [pipeline]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    depends_on: [zookeeper]
    healthcheck:
      test:
        [
          "CMD",
          "kafka-broker-api-versions",
          "--bootstrap-server",
          "localhost:9092",
        ]
      interval: 10s
      timeout: 10s
      retries: 5

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    networks: [pipeline]
    ports:
      - "8085:8080" # UI → http://localhost:8085
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on: [kafka]

  # ── Apache Spark ─────────────────────────────────────────────────────────────
  spark-master:
    image: bitnami/spark:3.5.0
    container_name: spark-master
    networks: [pipeline]
    ports:
      - "8081:8080" # Spark UI → http://localhost:8081
      - "7077:7077"
    environment:
      SPARK_MODE: master
      SPARK_RPC_AUTHENTICATION_ENABLED: no
      SPARK_RPC_ENCRYPTION_ENABLED: no
    volumes:
      - ./src:/opt/bitnami/spark/src
      - ./data:/opt/bitnami/spark/data

  spark-worker:
    image: bitnami/spark:3.5.0
    container_name: spark-worker
    networks: [pipeline]
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2
    depends_on: [spark-master]
    volumes:
      - ./src:/opt/bitnami/spark/src
      - ./data:/opt/bitnami/spark/data

  # ── Apache Airflow ──────────────────────────────────────────────────────────
  airflow:
    image: apache/airflow:2.8.0-python3.11
    container_name: airflow
    networks: [pipeline]
    ports:
      - "8080:8080" # Airflow UI → http://localhost:8080
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
    volumes:
      - ./dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - ./dbt:/opt/airflow/dbt
      - ./data:/opt/airflow/data
      - airflow_db:/opt/airflow
    command: >
      bash -c "
        pip install apache-airflow-providers-apache-spark &&
        airflow db init &&
        airflow users create --username admin --password admin
          --firstname Admin --lastname User --role Admin
          --email admin@example.com &&
        airflow webserver & airflow scheduler
      "
    depends_on: [kafka, spark-master, minio]

  # ── Streamlit Dashboard ─────────────────────────────────────────────────────
  dashboard:
    build:
      context: .
      dockerfile: Dockerfile.dashboard
    container_name: dashboard
    networks: [pipeline]
    ports:
      - "8501:8501" # Dashboard → http://localhost:8501
    volumes:
      - ./src/interfaces/dashboard:/app
      - ./data:/app/data
    command: streamlit run /app/app.py
```

---

## Sprint 0 — Setup (Friday evening, ~2h)

### Tasks

**[S0-01] pyproject.toml**

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
    "pytest>=7.0.0",
    "pytest-mock>=3.0.0",
]
```

**[S0-02] Uruchom środowisko**

```bash
docker compose up -d

# Sprawdź że wszystko działa
docker compose ps

# UI dostępne po ~60s:
# Airflow:   http://localhost:8080  (admin/admin)
# Spark:     http://localhost:8081
# MinIO:     http://localhost:9001  (minioadmin/minioadmin)
# Kafka UI:  http://localhost:8085
```

**[S0-03] Utwórz MinIO buckety**

```python
# scripts/setup_minio.py
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

for bucket in ["bronze", "silver", "gold"]:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")
```

**[S0-04] SparkSession factory z Delta Lake + MinIO**

```python
# src/infrastructure/spark/session.py
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "CarPricePipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://localhost:7077")
        # Delta Lake
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO as S3
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Packages
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4")
        .getOrCreate()
    )
```

**[S0-05] Delta Lake writer helper**

```python
# src/infrastructure/spark/delta_writer.py
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

class DeltaWriter:
    """Write and merge DataFrames to Delta Lake on MinIO."""

    def __init__(self, base_path: str = "s3a://"):
        self._base = base_path

    def write_bronze(self, df: DataFrame, table: str) -> None:
        """Append-only write to Bronze layer (raw, immutable)."""
        path = f"{self._base}bronze/{table}"
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
        merge_key: str = "id"
    ) -> None:
        """Upsert to Silver layer — idempotent."""
        path = f"{self._base}silver/{table}"
        spark = df.sparkSession

        if DeltaTable.isDeltaTable(spark, path):
            target = DeltaTable.forPath(spark, path)
            (
                target.alias("target")
                .merge(df.alias("source"), f"target.{merge_key} = source.{merge_key}")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            df.write.format("delta").mode("overwrite").save(path)
```

---

## Sprint 1 — Kafka Producer: Streaming Ingestion (Saturday morning, ~3h)

### Tasks

**[S1-01] Domain layer**

```python
# src/domain/value_objects/price.py
from pydantic import BaseModel, field_validator

class Price(BaseModel):
    amount: float

    @field_validator("amount")
    @classmethod
    def validate(cls, v: float) -> float:
        if v <= 0:      raise ValueError(f"Price must be positive, got {v}")
        if v > 500_000: raise ValueError(f"Price unrealistic: {v}")
        return round(v, 2)


# src/domain/value_objects/mileage.py
class Mileage(BaseModel):
    value: int

    @field_validator("value")
    @classmethod
    def validate(cls, v: int) -> int:
        if v < 0:         raise ValueError("Mileage cannot be negative")
        if v > 1_000_000: raise ValueError(f"Mileage unrealistic: {v}")
        return v


# src/domain/entities/listing.py
from dataclasses import dataclass
from typing import Optional
from src.domain.value_objects.price import Price
from src.domain.value_objects.mileage import Mileage

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

**[S1-02] Kafka producer — streams CSV rows as events**

```python
# src/infrastructure/kafka/producer.py
import json
import time
from kafka import KafkaProducer
from typing import Iterator

TOPIC = "car_listings_raw"

class ListingProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",              # wait for all replicas
            retries=3,
        )

    def send(self, listing_dict: dict) -> None:
        self._producer.send(
            topic=TOPIC,
            key=listing_dict["id"],
            value=listing_dict,
        )

    def send_batch(self, listings: Iterator[dict], delay_ms: int = 0) -> int:
        count = 0
        for listing in listings:
            self.send(listing)
            count += 1
            if delay_ms:
                time.sleep(delay_ms / 1000)
        self._producer.flush()
        return count

    def close(self) -> None:
        self._producer.close()


# src/application/produce_listings.py
import polars as pl
from pathlib import Path
from src.domain.entities.listing import Listing
from src.domain.value_objects.price import Price
from src.domain.value_objects.mileage import Mileage
from src.infrastructure.kafka.producer import ListingProducer

class ProduceListings:
    """Read CSV in batches, validate domain objects, stream to Kafka."""

    def __init__(self, producer: ListingProducer):
        self._producer = producer

    def execute(self, csv_path: Path, batch_size: int = 1000) -> dict:
        total, failed = 0, 0
        df = pl.read_csv(csv_path, infer_schema_length=10_000)

        for i in range(0, len(df), batch_size):
            batch = df.slice(i, batch_size)
            valid, errors = self._parse(batch)
            self._producer.send_batch(iter(valid))
            total += len(valid)
            failed += errors

        return {"produced": total, "failed": failed}

    def _parse(self, batch: pl.DataFrame) -> tuple[list[dict], int]:
        valid, errors = [], 0
        for row in batch.iter_rows(named=True):
            try:
                listing = Listing(
                    id=str(row["id"]),
                    make=str(row.get("manufacturer", "")).lower().strip(),
                    model=str(row.get("model", "")).lower().strip(),
                    year=int(row["year"]),
                    price=Price(amount=float(row["price"])),
                    mileage=Mileage(value=int(row.get("odometer", 0))),
                    state=str(row.get("state", "")),
                    condition=row.get("condition"),
                )
                valid.append(listing.to_dict())
            except Exception:
                errors += 1
        return valid, errors
```

---

## Sprint 2 — Spark Streaming → Bronze Delta (Saturday afternoon, ~3h)

### Tasks

**[S2-01] Spark Structured Streaming consumer**

```python
# src/application/stream_to_bronze.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from src.infrastructure.spark.session import get_spark_session

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

class StreamToBronze:
    """Consume Kafka topic → write to Bronze Delta Lake on MinIO."""

    BRONZE_PATH = "s3a://bronze/listings"
    CHECKPOINT  = "s3a://bronze/_checkpoints/listings"

    def __init__(self, spark: SparkSession):
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
            .select(F.from_json(
                F.col("value").cast("string"), KAFKA_SCHEMA
            ).alias("data"))
            .select("data.*")
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("kafka_partition", F.col("partition"))
        )

        query = (
            parsed.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.CHECKPOINT)
            .option("mergeSchema", "true")
            .start(self.BRONZE_PATH)
        )

        query.awaitTermination()
```

**[S2-02] Spark batch job — Bronze → Silver**

```python
# src/application/transform_silver.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.infrastructure.spark.session import get_spark_session
from src.infrastructure.spark.delta_writer import DeltaWriter

class TransformSilver:
    """Read Bronze Delta → apply business rules → write Silver Delta."""

    BRONZE_PATH = "s3a://bronze/listings"

    def __init__(self, spark: SparkSession, writer: DeltaWriter):
        self._spark  = spark
        self._writer = writer

    def execute(self) -> dict:
        bronze = self._spark.read.format("delta").load(self.BRONZE_PATH)
        silver = self._clean(bronze)
        silver = self._filter(silver)
        self._writer.upsert_silver(silver, "listings")
        return {"silver_count": silver.count()}

    def _clean(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("make",  F.lower(F.trim(F.col("make"))))
            .withColumn("model", F.lower(F.trim(F.col("model"))))
            .withColumn("state", F.upper(F.trim(F.col("state"))))
            .withColumn("log_price", F.log1p(F.col("price")))
            .withColumn("family_size",
                F.lit(1))   # placeholder — enrich from source if available
            .dropDuplicates(["id"])
        )

    def _filter(self, df: DataFrame) -> DataFrame:
        return df.filter(
            (F.col("price").between(500, 200_000))
            & (F.col("year").between(1990, 2024))
            & (F.col("mileage").between(0, 500_000))
            & (F.col("make").isNotNull())
            & (F.col("make") != "")
        )
```

---

## Sprint 3 — dbt Gold Layer (Saturday evening, ~2h)

### Tasks

**[S3-01] dbt profiles for Spark**

```yaml
# dbt/profiles.yml
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

**[S3-02] Gold mart models**

```sql
-- dbt/models/gold/mart_price_by_make.sql
{{ config(
    materialized='table',
    file_format='delta',
    location_root='s3a://gold'
) }}

SELECT
    make,
    COUNT(*)                        AS listing_count,
    ROUND(PERCENTILE(price, 0.5), 0) AS median_price,
    ROUND(AVG(price), 0)            AS avg_price,
    ROUND(STDDEV(price), 0)         AS price_stddev,
    ROUND(MIN(price), 0)            AS min_price,
    ROUND(MAX(price), 0)            AS max_price
FROM {{ source('silver', 'listings') }}
GROUP BY make
HAVING COUNT(*) >= 50
ORDER BY median_price DESC
```

```sql
-- dbt/models/gold/mart_price_by_year.sql
{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    make,
    year,
    COUNT(*)                         AS listing_count,
    ROUND(PERCENTILE(price, 0.5), 0) AS median_price
FROM {{ source('silver', 'listings') }}
WHERE make IN (
    SELECT make FROM {{ ref('mart_price_by_make') }}
    ORDER BY listing_count DESC
    LIMIT 10
)
GROUP BY make, year
ORDER BY make, year
```

```sql
-- dbt/models/gold/mart_price_by_state.sql
{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    state,
    COUNT(*)                         AS listing_count,
    ROUND(PERCENTILE(price, 0.5), 0) AS median_price,
    ROUND(AVG(price), 0)             AS avg_price
FROM {{ source('silver', 'listings') }}
GROUP BY state
ORDER BY median_price DESC
```

```sql
-- dbt/models/gold/mart_listings_summary.sql
{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    COUNT(*)                         AS total_listings,
    COUNT(DISTINCT make)             AS unique_makes,
    COUNT(DISTINCT state)            AS states_covered,
    ROUND(PERCENTILE(price, 0.5), 0) AS overall_median_price,
    ROUND(AVG(price), 0)             AS overall_avg_price,
    MIN(year)                        AS oldest_year,
    MAX(year)                        AS newest_year
FROM {{ source('silver', 'listings') }}
```

---

## Sprint 4 — Airflow DAG + Dashboard (Sunday, ~4h)

### Tasks

**[S4-01] Airflow DAG — full pipeline**

```python
# dags/car_price_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "kacper",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="car_price_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "de", "spark", "kafka"],
) as dag:

    # Step 1 — stream CSV rows to Kafka
    def produce():
        from pathlib import Path
        from src.infrastructure.kafka.producer import ListingProducer
        from src.application.produce_listings import ProduceListings

        producer = ListingProducer(bootstrap_servers="kafka:29092")
        use_case = ProduceListings(producer)
        result = use_case.execute(Path("/opt/airflow/data/raw/vehicles.csv"))
        print(f"Kafka produce: {result}")
        producer.close()

    kafka_produce = PythonOperator(
        task_id="kafka_produce",
        python_callable=produce,
    )

    # Step 2 — Spark Streaming: Kafka → Bronze Delta (runs 60s)
    spark_bronze = SparkSubmitOperator(
        task_id="spark_bronze",
        application="/opt/airflow/src/application/stream_to_bronze.py",
        conn_id="spark_default",
        packages="io.delta:delta-core_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )

    # Step 3 — Spark batch: Bronze → Silver Delta
    spark_silver = SparkSubmitOperator(
        task_id="spark_silver",
        application="/opt/airflow/src/application/transform_silver.py",
        conn_id="spark_default",
        packages="io.delta:delta-core_2.12:3.0.0",
    )

    # Step 4 — dbt: Silver → Gold marts
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    )

    # Step 5 — dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    kafka_produce >> spark_bronze >> spark_silver >> dbt_run >> dbt_test
```

**[S4-02] Streamlit dashboard**

```python
# src/interfaces/dashboard/app.py
import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px
import pandas as pd

st.set_page_config(page_title="Car Price Pipeline", page_icon="🚗", layout="wide")

@st.cache_resource
def get_spark():
    return SparkSession.builder.appName("Dashboard").master("local[2]").getOrCreate()

@st.cache_data
def load(path: str) -> pd.DataFrame:
    spark = get_spark()
    return spark.read.format("delta").load(path).toPandas()

st.title("🚗 Car Price Pipeline")
st.caption("Powered by Spark + Delta Lake + Kafka + dbt | Local Docker Stack")

# ── KPIs ──────────────────────────────────────────────────────────────────────
summary = load("s3a://gold/mart_listings_summary")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Listings",  f"{summary['total_listings'][0]:,}")
c2.metric("Unique Makes",    summary['unique_makes'][0])
c3.metric("States Covered",  summary['states_covered'][0])
c4.metric("Median Price",   f"${summary['overall_median_price'][0]:,}")

st.divider()

# ── Price by Make ─────────────────────────────────────────────────────────────
st.subheader("Median Price by Make — Top 20")
makes = load("s3a://gold/mart_price_by_make")
fig = px.bar(
    makes.head(20), x="make", y="median_price",
    color="listing_count", color_continuous_scale="Blues"
)
st.plotly_chart(fig, use_container_width=True)

# ── Depreciation Curve ────────────────────────────────────────────────────────
st.subheader("Price Depreciation by Year")
depr = load("s3a://gold/mart_price_by_year")
fig2 = px.line(depr, x="year", y="median_price", color="make")
st.plotly_chart(fig2, use_container_width=True)

# ── Geographic ────────────────────────────────────────────────────────────────
st.subheader("Median Price by State")
geo = load("s3a://gold/mart_price_by_state")
fig3 = px.choropleth(
    geo, locations="state", locationmode="USA-states",
    color="median_price", scope="usa", color_continuous_scale="Viridis"
)
st.plotly_chart(fig3, use_container_width=True)
```

---

## Sprint 5 — Tests + README (Sunday evening, ~2h)

**[S5-01] Unit tests**

```python
# tests/unit/domain/test_price.py
import pytest
from src.domain.value_objects.price import Price

@pytest.mark.parametrize("amount", [500, 12000, 199999])
def test_valid_prices(amount):
    assert Price(amount=amount).amount == amount

@pytest.mark.parametrize("amount", [0, -100, 600_000])
def test_invalid_prices(amount):
    with pytest.raises(Exception):
        Price(amount=amount)
```

```python
# tests/integration/test_kafka_producer.py
from unittest.mock import MagicMock, patch
from src.infrastructure.kafka.producer import ListingProducer

def test_send_batch_returns_count():
    with patch("src.infrastructure.kafka.producer.KafkaProducer"):
        producer = ListingProducer()
        rows = [{"id": str(i), "make": "toyota"} for i in range(10)]
        count = producer.send_batch(iter(rows))
        assert count == 10
```

**[S5-02] Uruchom testy**

```bash
pytest tests/unit -v
dbt test
```

---

## Uruchomienie w 3 krokach

```bash
# 1. Start całego środowiska
docker compose up -d && sleep 60

# 2. Setup MinIO buckety
python scripts/setup_minio.py

# 3. Trigger pipeline (lub przez Airflow UI → localhost:8080)
python -m src.interfaces.cli.pipeline_cli run
```

---

## Lokalne UI — wszystko na localhost

| Service       | URL                   | Login                   |
| ------------- | --------------------- | ----------------------- |
| Airflow       | http://localhost:8080 | admin / admin           |
| Spark UI      | http://localhost:8081 | —                       |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI      | http://localhost:8085 | —                       |
| Dashboard     | http://localhost:8501 | —                       |

---

## Claude Code Prompts

```
Implement Sprint 0: create full project structure, pyproject.toml with all dependencies, docker-compose.yml exactly as specified, SparkSession factory with Delta Lake and MinIO config, and DeltaWriter helper class.
```

```
Implement the domain layer: Price and Mileage value objects with Pydantic validation, Listing entity with to_dict() method. Include all unit tests as specified.
```

```
Implement the Kafka producer and ProduceListings use case. Read CSV with Polars in batches of 1000 rows, validate via domain objects, stream valid rows to Kafka topic car_listings_raw.
```

```
Implement StreamToBronze using Spark Structured Streaming. Consume from Kafka, parse JSON with the defined schema, add ingested_at timestamp, write to Delta Lake on MinIO at s3a://bronze/listings with checkpointing.
```

```
Implement TransformSilver batch job: read Bronze Delta, clean strings, apply business rule filters, upsert to Silver Delta using DeltaWriter.upsert_silver().
```

```
Create all four dbt Gold models: mart_price_by_make, mart_price_by_year, mart_price_by_state, mart_listings_summary. All materialized as Delta tables on s3a://gold.
```

```
Implement the Airflow DAG with 5 tasks: kafka_produce → spark_bronze → spark_silver → dbt_run → dbt_test. Use SparkSubmitOperator for Spark jobs and PythonOperator for Kafka.
```

```
Build the Streamlit dashboard reading from Delta Lake via Spark. Include KPI metrics row, bar chart for price by make, line chart for depreciation, and US choropleth map.
```
