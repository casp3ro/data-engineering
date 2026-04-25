{{ config(materialized='table', file_format='delta') }}

SELECT *
FROM {{ ref('stg_listings') }}
WHERE
    price   BETWEEN 500   AND 200000
    AND year BETWEEN 1990  AND 2024
    AND mileage BETWEEN 0  AND 500000
    AND make NOT IN ('', 'unknown')
    AND state IS NOT NULL
