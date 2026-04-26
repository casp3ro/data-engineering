{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM read_parquet('../data/silver/listings/*.parquet')
),
cleaned AS (
    SELECT
        id,
        LOWER(TRIM(manufacturer))   AS make,
        LOWER(TRIM(model))          AS model,
        CAST(year AS INTEGER)       AS year,
        CAST(price AS DOUBLE)       AS price,
        CAST(odometer AS INTEGER)   AS mileage,
        UPPER(TRIM(state))          AS state,
        condition,
        ingested_at
    FROM source
    WHERE price IS NOT NULL
      AND year  IS NOT NULL
      AND manufacturer IS NOT NULL
)
SELECT * FROM cleaned
