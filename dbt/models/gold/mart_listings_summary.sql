{{ config(materialized='table', file_format='delta', location_root='s3a://gold') }}

SELECT
    COUNT(*)                         AS total_listings,
    COUNT(DISTINCT make)             AS unique_makes,
    COUNT(DISTINCT state)            AS states_covered,
    ROUND(MEDIAN(price), 0) AS overall_median_price,
    ROUND(AVG(price), 0)             AS overall_avg_price,
    MIN(year)                        AS oldest_year,
    MAX(year)                        AS newest_year
FROM {{ ref('int_listings_valid') }}
