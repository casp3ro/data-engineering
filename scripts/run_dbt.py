"""
scripts/run_dbt.py
"""
import subprocess
import sys
from pathlib import Path
from pyspark.sql import SparkSession

DBT_DIR = Path(__file__).parent.parent / "dbt"

SILVER_PATH = "s3a://silver/listings"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RegisterTables")
        .master("local[*]")
        .config("spark.sql.extensions",           "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",    "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key",  "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",  "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",        "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.driver.memory", "2g")
        .enableHiveSupport()
        .getOrCreate()
    )


def register_tables() -> None:
    print("Registering Delta tables in Spark catalog...")
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver.listings
        USING delta
        LOCATION '{SILVER_PATH}'
    """)
    print("Registered: silver.listings")
    spark.stop()


def run(cmd: str) -> None:
    print(f"\n$ {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=DBT_DIR)
    if result.returncode != 0:
        print(f"ERROR: command failed with code {result.returncode}")
        sys.exit(result.returncode)


def main() -> None:
    register_tables()
    run("dbt run --profiles-dir .")
    run("dbt test --profiles-dir .")
    print("\ndbt complete.")


if __name__ == "__main__":
    main()