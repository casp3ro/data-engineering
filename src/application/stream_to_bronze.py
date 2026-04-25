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
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def run(self) -> None:
        raw = (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
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

        query = (
            parsed.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT)
            .option("mergeSchema", "true")
            .start(BRONZE_PATH)
        )

        query.awaitTermination()
