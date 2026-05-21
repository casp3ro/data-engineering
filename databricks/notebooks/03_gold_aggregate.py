# Databricks notebook — 03_gold_aggregate
# Role: Silver Delta → Gold aggregation tables (business-ready analytics layer)
# Architecture: reads Silver batch → produces 3 Gold Delta tables for dbt + Streamlit
#
# TODO:
#   - gold/price_by_brand/ — avg/median/min/max price per manufacturer (count >= 50)
#   - gold/price_by_year/  — avg price + count per year
#   - gold/price_by_condition/ — avg price per condition × manufacturer

# SECTION: imports & paths
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    percentile_approx,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)

SILVER_PATH = "dbfs:/pipelines/car-price/silver/listings/"
GOLD_BASE = "dbfs:/pipelines/car-price/gold/"

MIN_COUNT_BRAND = 50  # minimum listings to include a manufacturer

spark: SparkSession = spark  # noqa: F821

silver = spark.read.format("delta").load(SILVER_PATH)

# SECTION: gold/price_by_brand
price_by_brand = (
    silver
    .filter(col("manufacturer").isNotNull() & (col("manufacturer") != ""))
    .groupBy("manufacturer")
    .agg(
        avg("price").alias("avg_price"),
        percentile_approx("price", 0.5).alias("median_price"),
        count("*").alias("count"),
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
    )
    .filter(col("count") >= MIN_COUNT_BRAND)
    .orderBy(col("count").desc())
    .withColumn("_gold_created_at", current_timestamp())
)

price_by_brand.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}price_by_brand/")
brand_count = price_by_brand.count()

# SECTION: gold/price_by_year
price_by_year = (
    silver
    .filter(col("year").isNotNull())
    .groupBy("year")
    .agg(
        avg("price").alias("avg_price"),
        count("*").alias("count"),
    )
    .orderBy("year")
    .withColumn("_gold_created_at", current_timestamp())
)

price_by_year.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}price_by_year/")
year_count = price_by_year.count()

# SECTION: gold/price_by_condition
price_by_condition = (
    silver
    .filter(col("condition").isNotNull() & (col("condition") != ""))
    .groupBy("condition", "manufacturer")
    .agg(
        avg("price").alias("avg_price"),
        count("*").alias("count"),
    )
    .withColumn("_gold_created_at", current_timestamp())
)

price_by_condition.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}price_by_condition/")
condition_count = price_by_condition.count()

# SECTION: report metrics
dbutils.notebook.exit(json.dumps({  # noqa: F821
    "price_by_brand_rows": brand_count,
    "price_by_year_rows": year_count,
    "price_by_condition_rows": condition_count,
}))
