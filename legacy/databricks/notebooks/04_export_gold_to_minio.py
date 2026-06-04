# Databricks notebook — 04_export_gold_to_minio
# Copies Gold Delta tables from DBFS to MinIO (S3-compatible) for local dbt-duckdb.
#
# Prerequisites (Databricks secret scope: car-price-pipeline):
#   MINIO_ENDPOINT       e.g. host.docker.internal:9000 or ngrok tunnel host:port
#   MINIO_ACCESS_KEY
#   MINIO_SECRET_KEY
#
# Run after 03_gold_aggregate. Airflow dbt task uses PIPELINE_MODE=databricks.

# SECTION: configure S3A → MinIO
import json

endpoint = dbutils.secrets.get("car-price-pipeline", "MINIO_ENDPOINT")  # noqa: F821
access_key = dbutils.secrets.get("car-price-pipeline", "MINIO_ACCESS_KEY")  # noqa: F821
secret_key = dbutils.secrets.get("car-price-pipeline", "MINIO_SECRET_KEY")  # noqa: F821

spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")  # noqa: F821
spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)  # noqa: F821
spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)  # noqa: F821
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")  # noqa: F821
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # noqa: F821

DBFS_GOLD = "dbfs:/pipelines/car-price/gold/"
S3_GOLD = "s3a://gold/"

TABLES = [
    "price_by_brand",
    "price_by_year",
    "price_by_condition",
    "listings_summary",
    "price_by_state",
    "price_by_make_year",
]

exported = {}
for name in TABLES:
    src = f"{DBFS_GOLD}{name}/"
    dest = f"{S3_GOLD}{name}/"
    df = spark.read.format("delta").load(src)  # noqa: F821
    count = df.count()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dest)
    exported[name] = count

dbutils.notebook.exit(json.dumps({"exported": exported}))  # noqa: F821
