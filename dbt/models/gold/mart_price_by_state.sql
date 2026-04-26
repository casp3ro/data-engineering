{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    state,
    COUNT(*)                         AS listing_count,
    ROUND(MEDIAN(price), 0) AS median_price,
    ROUND(AVG(price), 0)             AS avg_price
FROM {{ ref('int_listings_valid') }}
GROUP BY state
ORDER BY median_price DESC
