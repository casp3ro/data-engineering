import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("mileage", IntegerType(), True),
        StructField("state", StringType(), True),
        StructField("condition", StringType(), True),
    ]
)

BRONZE_PATH = "s3a://bronze/listings"
CHECKPOINT = "s3a://bronze/_checkpoints/listings"


class StreamToBronze:
    def __init__(self, spark: SparkSession, bootstrap_servers: str | None = None) -> None:
        self._spark = spark
        self._bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        )

    def run(self) -> None:
        raw = (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self._bootstrap_servers)
            .option("subscribe", "car_listings_raw")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = (
            raw.select(F.from_json(F.col("value").cast("string"), KAFKA_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("ingested_at", F.current_timestamp())
        )

        writer = (
            parsed.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT)
            .option("mergeSchema", "true")
        )

        # In Airflow we want a finite task that completes deterministically.
        # Prefer availableNow on modern Spark, fall back to once for older runtimes.
        try:
            writer = writer.trigger(availableNow=True)
        except TypeError:
            writer = writer.trigger(once=True)

        query = writer.start(BRONZE_PATH)

        query.awaitTermination()
