{{ config(materialized='table') }}

{% if var('pipeline_mode') == 'databricks' %}

SELECT
    total_listings,
    unique_makes,
    states_covered,
    ROUND(overall_median_price, 0) AS overall_median_price,
    ROUND(overall_avg_price, 0)    AS overall_avg_price,
    oldest_year,
    newest_year
FROM {{ ref('stg_listings_summary') }}

{% else %}

SELECT
    COUNT(*)                         AS total_listings,
    COUNT(DISTINCT make)             AS unique_makes,
    COUNT(DISTINCT state)            AS states_covered,
    ROUND(MEDIAN(price), 0)          AS overall_median_price,
    ROUND(AVG(price), 0)             AS overall_avg_price,
    MIN(year)                        AS oldest_year,
    MAX(year)                        AS newest_year
FROM {{ ref('int_listings_valid') }}

{% endif %}
