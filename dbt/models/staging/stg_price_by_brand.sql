-- staging/stg_price_by_brand: read Gold price_by_brand from Databricks DBFS via delta_scan
-- Filters out nulls and enforces min_count threshold defined in dbt vars.

SELECT
    manufacturer,
    avg_price,
    median_price,
    count,
    min_price,
    max_price
FROM delta_scan('dbfs:/pipelines/car-price/gold/price_by_brand/')
WHERE manufacturer IS NOT NULL
  AND count >= {{ var('min_count') }}
