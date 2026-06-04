{{ config(materialized='view') }}

WITH source AS (
    {{ delta_scan_from_path('silver_listings') }}
)
SELECT
    id,
    make,
    model,
    year,
    price,
    mileage,
    state,
    condition,
    ingested_at
FROM source
WHERE price IS NOT NULL
  AND year  IS NOT NULL
  AND make  IS NOT NULL
