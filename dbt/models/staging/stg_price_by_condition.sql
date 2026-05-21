-- staging/stg_price_by_condition: read Gold price_by_condition from Databricks DBFS

SELECT
    condition,
    manufacturer,
    avg_price,
    count
FROM delta_scan('dbfs:/pipelines/car-price/gold/price_by_condition/')
WHERE condition IS NOT NULL
  AND manufacturer IS NOT NULL
