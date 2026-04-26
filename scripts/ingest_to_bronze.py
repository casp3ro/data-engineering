"""
scripts/ingest_to_bronze.py
Read vehicles.csv with Spark, write Delta to MinIO bronze bucket.
No Kafka needed.
"""
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CSV_PATH  = "data/raw/vehicles.csv"
BRONZE_PATH = "s3a://bronze/listings"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IngestToBronze")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.sql.extensions",        "io.delta.sql.DeltaSparkSessionExtension")
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


def main() -> None:
    if not Path(CSV_PATH).exists():
        print(f"ERROR: {CSV_PATH} not found. Download from:")
        print("https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data")
        sys.exit(1)

    print("Starting Spark...")
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading {CSV_PATH}...")
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    print(f"Rows loaded: {df.count():,}")

    # Add ingestion metadata
    df = df.withColumn("ingested_at", F.current_timestamp())

    print(f"Writing Delta to {BRONZE_PATH}...")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(BRONZE_PATH)
    )

    print("Bronze layer ready.")
    print(f"Schema: {', '.join(df.columns)}")
    spark.stop()


if __name__ == "__main__":
    main()