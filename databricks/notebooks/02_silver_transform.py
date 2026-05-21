# Databricks notebook — 02_silver_transform
# Role: Bronze Delta → Silver Delta (cleaning, standardisation, deduplication)
# Architecture: reads Bronze batch → applies business-rule filters → writes Silver Delta
#
# TODO:
#   - Verify Bronze path contains data before running
#   - Silver overwrites on each run (overwriteSchema=true handles schema evolution)

# SECTION: imports & constants
import json

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lower, trim
from pyspark.sql.window import Window

BRONZE_PATH = "dbfs:/pipelines/car-price/bronze/listings/"
SILVER_PATH = "dbfs:/pipelines/car-price/silver/listings/"

# Business-rule bounds — single source of truth; kept in sync with src/domain/constants.py
MIN_PRICE: float = 500.0
MAX_PRICE: float = 200_000.0
MIN_YEAR: int = 1980
MAX_YEAR: int = 2025
MIN_MILEAGE: float = 0.0
MAX_MILEAGE: float = 500_000.0

spark: SparkSession = spark  # noqa: F821

# SECTION: read Bronze (batch)
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
input_rows = bronze_df.count()

# SECTION: clean & standardise
string_cols = ["manufacturer", "model", "condition", "fuel", "transmission", "drive", "state"]

cleaned = bronze_df
for c in string_cols:
    cleaned = cleaned.withColumn(c, trim(lower(col(c))))

# SECTION: filter — apply business rules
filtered = (
    cleaned
    .filter(col("price").between(MIN_PRICE, MAX_PRICE))
    .filter(col("year").between(MIN_YEAR, MAX_YEAR))
    .filter(col("mileage").isNull() | col("mileage").between(MIN_MILEAGE, MAX_MILEAGE))
)

# SECTION: deduplicate — keep latest record per id
window = Window.partitionBy("id").orderBy(col("_ingested_at").desc())
deduplicated = (
    filtered
    .withColumn("_row_num", F.row_number().over(window))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
    .withColumn("_silver_processed_at", current_timestamp())
)

output_rows = deduplicated.count()
filtered_rows = input_rows - output_rows

# SECTION: write Silver Delta (full overwrite — schema may change between runs)
(
    deduplicated.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

# SECTION: report metrics
dbutils.notebook.exit(json.dumps({  # noqa: F821
    "input_rows": input_rows,
    "output_rows": output_rows,
    "filtered_rows": filtered_rows,
}))
