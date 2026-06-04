{{ config(materialized='table') }}

{% if var('pipeline_mode') == 'databricks' %}

SELECT
    state,
    listing_count,
    ROUND(median_price, 0) AS median_price,
    ROUND(avg_price, 0)    AS avg_price
FROM {{ ref('stg_price_by_state') }}
ORDER BY median_price DESC

{% else %}

SELECT
    state,
    COUNT(*)                         AS listing_count,
    ROUND(MEDIAN(price), 0)          AS median_price,
    ROUND(AVG(price), 0)             AS avg_price
FROM {{ ref('int_listings_valid') }}
GROUP BY state
ORDER BY median_price DESC

{% endif %}
