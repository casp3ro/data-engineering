{{ config(materialized='table') }}

{% if var('pipeline_mode') == 'databricks' %}

SELECT
    make,
    year,
    listing_count,
    ROUND(median_price, 0) AS median_price
FROM {{ ref('stg_price_by_make_year') }}
ORDER BY make, year

{% else %}

WITH top_makes AS (
    SELECT make
    FROM {{ ref('mart_price_by_make') }}
    ORDER BY listing_count DESC
    LIMIT {{ var('top_makes_limit', 10) }}
)
SELECT
    src.make,
    src.year,
    COUNT(*)                    AS listing_count,
    ROUND(MEDIAN(src.price), 0) AS median_price
FROM {{ ref('int_listings_valid') }} AS src
INNER JOIN top_makes ON src.make = top_makes.make
GROUP BY src.make, src.year
ORDER BY src.make, src.year

{% endif %}
