# Databricks notebook — 01_bronze_ingest
# Role: Kafka topic → Bronze Delta Layer (raw ingestion, no business transforms)
# Architecture: Kafka (Docker/Confluent Cloud) → Structured Streaming → Delta on DBFS
#
# TODO:
#   - Set KAFKA_BOOTSTRAP_SERVERS in Databricks secret scope "car-price-pipeline"
#   - Upload listing.avsc to DBFS: dbfs:/pipelines/car-price/schemas/listing.avsc
#   - Attach a cluster with kafka connector jars or use Confluent Cloud connector

# SECTION: imports & config
import json

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, lit

BRONZE_PATH = "dbfs:/pipelines/car-price/bronze/listings/"
CHECKPOINT_PATH = "dbfs:/pipelines/car-price/checkpoints/bronze/"
SCHEMA_PATH = "dbfs:/pipelines/car-price/schemas/listing.avsc"
KAFKA_TOPIC = "car-listings"

# SECTION: read Avro schema from DBFS
avro_schema_str = dbutils.fs.head(SCHEMA_PATH)  # noqa: F821  # dbutils injected by Databricks

# SECTION: Kafka structured streaming read
spark: SparkSession = spark  # noqa: F821  # spark injected by Databricks

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get("car-price-pipeline", "KAFKA_BOOTSTRAP_SERVERS"))  # noqa: F821
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# SECTION: deserialise Avro payload + add metadata columns
parsed = (
    raw_stream.select(
        from_avro(col("value"), avro_schema_str).alias("data"),
        col("partition").alias("_partition"),
        col("offset").alias("_offset"),
    )
    .select("data.*", "_partition", "_offset")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("kafka"))
)

# SECTION: write to Bronze Delta
(
    parsed.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start(BRONZE_PATH)
    .awaitTermination()
)

# SECTION: report metrics
bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
dbutils.notebook.exit(json.dumps({"bronze_row_count": bronze_count}))  # noqa: F821
