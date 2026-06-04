{{ config(materialized='table') }}

{% if var('pipeline_mode') == 'databricks' %}

SELECT
    manufacturer AS make,
    count            AS listing_count,
    ROUND(median_price, 0) AS median_price,
    ROUND(avg_price, 0)    AS avg_price,
    ROUND((max_price - min_price) / NULLIF(avg_price, 0), 4) AS price_stddev,
    ROUND(min_price, 0) AS min_price,
    ROUND(max_price, 0) AS max_price
FROM {{ ref('stg_price_by_brand') }}
ORDER BY median_price DESC

{% else %}

SELECT
    make,
    COUNT(*)                         AS listing_count,
    ROUND(MEDIAN(price), 0)          AS median_price,
    ROUND(AVG(price), 0)             AS avg_price,
    ROUND(STDDEV(price), 0)          AS price_stddev,
    ROUND(MIN(price), 0)             AS min_price,
    ROUND(MAX(price), 0)             AS max_price
FROM {{ ref('int_listings_valid') }}
GROUP BY make
HAVING COUNT(*) >= {{ var('min_count') }}
ORDER BY median_price DESC

{% endif %}
