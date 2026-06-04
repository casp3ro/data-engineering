# Databricks notebook — 03_gold_aggregate
# Role: Silver Delta → Gold aggregation tables (business-ready analytics layer)

# SECTION: imports & paths
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    length,
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

MIN_COUNT_BRAND = 50

spark: SparkSession = spark  # noqa: F821

silver = spark.read.format("delta").load(SILVER_PATH)

# SECTION: gold/price_by_brand
price_by_brand = (
    silver.filter(col("manufacturer").isNotNull() & (col("manufacturer") != ""))
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
    silver.filter(col("year").isNotNull())
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
    silver.filter(col("condition").isNotNull() & (col("condition") != ""))
    .groupBy("condition", "manufacturer")
    .agg(
        avg("price").alias("avg_price"),
        count("*").alias("count"),
    )
    .withColumn("_gold_created_at", current_timestamp())
)

price_by_condition.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}price_by_condition/")
condition_count = price_by_condition.count()

# SECTION: gold/listings_summary (dashboard)
listings_summary = silver.agg(
    count("*").alias("total_listings"),
    countDistinct("manufacturer").alias("unique_makes"),
    countDistinct("state").alias("states_covered"),
    percentile_approx("price", 0.5).alias("overall_median_price"),
    avg("price").alias("overall_avg_price"),
    spark_min("year").alias("oldest_year"),
    spark_max("year").alias("newest_year"),
).withColumn("_gold_created_at", current_timestamp())

listings_summary.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}listings_summary/")

# SECTION: gold/price_by_state (dashboard)
price_by_state = (
    silver.filter(col("state").isNotNull() & (length(col("state")) == 2))
    .groupBy("state")
    .agg(
        count("*").alias("listing_count"),
        percentile_approx("price", 0.5).alias("median_price"),
        avg("price").alias("avg_price"),
    )
    .withColumn("_gold_created_at", current_timestamp())
)

price_by_state.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}price_by_state/")

# SECTION: gold/price_by_make_year (dashboard)
top_manufacturers = [
    row.manufacturer
    for row in price_by_brand.orderBy(col("count").desc()).limit(10).collect()
]

price_by_make_year = (
    silver.filter(col("manufacturer").isin(top_manufacturers) & col("year").isNotNull())
    .groupBy(col("manufacturer").alias("make"), "year")
    .agg(
        count("*").alias("listing_count"),
        percentile_approx("price", 0.5).alias("median_price"),
    )
    .withColumn("_gold_created_at", current_timestamp())
)

price_by_make_year.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}price_by_make_year/")

# SECTION: report metrics
dbutils.notebook.exit(json.dumps({  # noqa: F821
    "price_by_brand_rows": brand_count,
    "price_by_year_rows": year_count,
    "price_by_condition_rows": condition_count,
}))
