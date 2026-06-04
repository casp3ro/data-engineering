import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

BRONZE_PATH = "s3a://bronze/listings"
CHECKPOINT = "s3a://bronze/_checkpoints/listings"
_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "listing.avsc"


class StreamToBronze:
    def __init__(self, spark: SparkSession, bootstrap_servers: str | None = None) -> None:
        self._spark = spark
        self._bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        )
        self._topic = os.getenv("KAFKA_TOPIC", "car-listings")
        self._avro_schema = _SCHEMA_PATH.read_text()

    def run(self) -> None:
        raw = (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self._bootstrap_servers)
            .option("subscribe", self._topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = (
            raw.select(
                from_avro(F.col("value"), self._avro_schema).alias("data"),
                F.col("partition").alias("_partition"),
                F.col("offset").alias("_offset"),
            )
            .select("data.*", "_partition", "_offset")
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("kafka"))
        )

        writer = (
            parsed.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT)
            .option("mergeSchema", "true")
        )

        try:
            writer = writer.trigger(availableNow=True)
        except TypeError:
            writer = writer.trigger(once=True)

        query = writer.start(BRONZE_PATH)
        query.awaitTermination()
