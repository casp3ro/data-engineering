"""
scripts/transform_silver.py
Read Bronze Delta from MinIO, clean and filter, write Silver Delta.
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.domain.constants import (
    EXCLUDED_MAKES,
    MAX_MILEAGE,
    MAX_PRICE,
    MAX_YEAR,
    MIN_MILEAGE,
    MIN_PRICE,
    MIN_YEAR,
)
from src.infrastructure.spark.session import get_spark_session

logger = logging.getLogger(__name__)

BRONZE_PATH = "s3a://bronze/listings"
SILVER_PATH = "s3a://silver/listings"


def clean(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("make", F.lower(F.trim(F.col("manufacturer"))))
        .withColumn("model", F.lower(F.trim(F.col("model"))))
        .withColumn("state", F.upper(F.trim(F.col("state"))))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("mileage", F.col("mileage").cast("integer"))
        .withColumn("year", F.col("year").cast("integer"))
        .withColumn("log_price", F.log1p(F.col("price")))
        .dropDuplicates(["id"])
    )


def filter_valid(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("price").between(MIN_PRICE, MAX_PRICE)
        & F.col("year").between(MIN_YEAR, MAX_YEAR)
        & F.col("mileage").between(MIN_MILEAGE, MAX_MILEAGE)
        & F.col("make").isNotNull()
        & (~F.col("make").isin(list(EXCLUDED_MAKES)))
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    logger.info("Starting Spark...")
    spark = get_spark_session("TransformSilver")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading Bronze from %s...", BRONZE_PATH)
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    logger.info("Bronze rows: %s", f"{bronze.count():,}")

    logger.info("Cleaning and filtering...")
    silver = filter_valid(clean(bronze))
    logger.info("Silver rows: %s", f"{silver.count():,}")

    logger.info("Writing Silver to %s...", SILVER_PATH)
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH)
    )

    logger.info("Silver layer ready.")
    spark.stop()


if __name__ == "__main__":
    main()
