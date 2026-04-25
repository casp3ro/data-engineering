{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    make,
    COUNT(*)                         AS listing_count,
    ROUND(PERCENTILE(price, 0.5), 0) AS median_price,
    ROUND(AVG(price), 0)             AS avg_price,
    ROUND(STDDEV(price), 0)          AS price_stddev,
    ROUND(MIN(price), 0)             AS min_price,
    ROUND(MAX(price), 0)             AS max_price
FROM {{ ref('int_listings_valid') }}
GROUP BY make
HAVING COUNT(*) >= 50
ORDER BY median_price DESC
