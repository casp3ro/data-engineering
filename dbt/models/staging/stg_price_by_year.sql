-- staging/stg_price_by_year: read Gold price_by_year from Databricks DBFS via delta_scan
-- Enforces year bounds defined in dbt vars.

SELECT
    year,
    avg_price,
    count
FROM delta_scan('dbfs:/pipelines/car-price/gold/price_by_year/')
WHERE year BETWEEN {{ var('min_year') }} AND {{ var('max_year') }}
  AND year IS NOT NULL
