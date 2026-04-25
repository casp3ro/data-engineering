{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    make,
    year,
    COUNT(*)                         AS listing_count,
    ROUND(PERCENTILE(price, 0.5), 0) AS median_price
FROM {{ ref('int_listings_valid') }}
WHERE make IN (
    SELECT make FROM {{ ref('mart_price_by_make') }}
    ORDER BY listing_count DESC LIMIT 10
)
GROUP BY make, year
ORDER BY make, year
