import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "CarPricePipeline") -> SparkSession:
    # Defaults assume you're submitting from the host into the Docker Spark cluster:
    # - driver connects to the published Spark master port on localhost
    # - executors (running in containers) can reach MinIO via host.docker.internal on macOS
    # Override via env vars when running fully inside Docker network.
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")
    minio_endpoint = os.getenv("MINIO_S3_ENDPOINT", "http://host.docker.internal:9000")
    return (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )
