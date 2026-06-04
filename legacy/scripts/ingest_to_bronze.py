"""
scripts/ingest_to_bronze.py
Read vehicles.csv with Spark, write Delta to MinIO bronze bucket.
No Kafka needed.
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.infrastructure.spark.session import get_spark_session

logger = logging.getLogger(__name__)

CSV_PATH = "data/raw/vehicles.csv"
BRONZE_PATH = "s3a://bronze/listings"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    if not Path(CSV_PATH).exists():
        logger.error("CSV not found: %s", CSV_PATH)
        logger.error(
            "Download from: https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data"
        )
        sys.exit(1)

    logger.info("Starting Spark...")
    spark = get_spark_session("IngestToBronze")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading %s...", CSV_PATH)
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    df = df.withColumnRenamed("odometer", "mileage")
    logger.info("Rows loaded: %s", f"{df.count():,}")

    df = df.withColumn("ingested_at", F.current_timestamp())

    logger.info("Writing Delta to %s...", BRONZE_PATH)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
    )

    logger.info("Bronze layer ready. Schema: %s", ", ".join(df.columns))
    spark.stop()


if __name__ == "__main__":
    main()
