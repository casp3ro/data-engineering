"""
scripts/transform_silver.py
Read Bronze Delta from MinIO, clean and filter, write Silver Delta.
"""
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


BRONZE_PATH = "s3a://bronze/listings"
SILVER_PATH = "data/silver/listings"   # lokalnie zamiast MinIO


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TransformSilver")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.sql.extensions",           "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",    os.getenv("MINIO_S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key",  "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",  "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",        "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def clean(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("make",      F.lower(F.trim(F.col("manufacturer"))))
        .withColumn("model",     F.lower(F.trim(F.col("model"))))
        .withColumn("state",     F.upper(F.trim(F.col("state"))))
        .withColumn("price",     F.col("price").cast("double"))
        .withColumn("odometer",  F.col("odometer").cast("integer"))
        .withColumn("year",      F.col("year").cast("integer"))
        .withColumn("log_price", F.log1p(F.col("price")))
        .dropDuplicates(["id"])
    )


def filter_valid(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("price").between(500, 200_000)
        & F.col("year").between(1990, 2024)
        & F.col("odometer").between(0, 500_000)
        & F.col("make").isNotNull()
        & (F.col("make") != "")
    )


def main() -> None:
    print("Starting Spark...")
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading Bronze from {BRONZE_PATH}...")
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze rows: {bronze.count():,}")

    print("Cleaning and filtering...")
    silver = filter_valid(clean(bronze))
    print(f"Silver rows: {silver.count():,}")

    print(f"Writing Silver to {SILVER_PATH}...")
    (
        silver.write
        .format("parquet")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print("Silver layer ready.")
    spark.stop()


if __name__ == "__main__":
    main()