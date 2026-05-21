-- marts/mart_price_overview: top 20 manufacturers with their best (lowest avg price) year.
-- Joins brand and year aggregations to produce a single overview table for dashboards.

WITH top_brands AS (
    SELECT manufacturer, avg_price, count
    FROM {{ ref('stg_price_by_brand') }}
    ORDER BY count DESC
    LIMIT 20
),

best_year_per_brand AS (
    SELECT
        b.manufacturer,
        y.year,
        y.avg_price AS year_avg_price,
        ROW_NUMBER() OVER (
            PARTITION BY b.manufacturer
            ORDER BY y.avg_price ASC
        ) AS rn
    FROM top_brands b
    -- Cross-join with year data; no direct FK — both are aggregated gold tables
    CROSS JOIN {{ ref('stg_price_by_year') }} y
)

SELECT
    tb.manufacturer,
    tb.avg_price,
    tb.count,
    bybr.year AS best_value_year
FROM top_brands tb
LEFT JOIN best_year_per_brand bybr
    ON tb.manufacturer = bybr.manufacturer
    AND bybr.rn = 1
ORDER BY tb.count DESC
