{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'listings') }}
),
cleaned AS (
    SELECT
        id,
        {{ clean_string('make') }}  AS make,
        {{ clean_string('model') }} AS model,
        year,
        price,
        mileage,
        UPPER(state)                AS state,
        condition,
        ingested_at
    FROM source
    WHERE price IS NOT NULL
      AND year  IS NOT NULL
      AND make  IS NOT NULL
)
SELECT * FROM cleaned
