-- marts/mart_brand_ranking: value score = normalised inverse of avg_price × log(count)
-- Higher score = better value (lower price, more listings = more reliable signal).

WITH scored AS (
    SELECT
        manufacturer,
        avg_price,
        count,
        -- value_score: high count + low price → high score
        -- Normalise so score is always positive and comparable across brands
        ROUND(
            (1.0 / NULLIF(avg_price, 0)) * LN(GREATEST(count, 1)) * 1_000_000,
            4
        ) AS value_score
    FROM {{ ref('stg_price_by_brand') }}
)

SELECT
    manufacturer,
    avg_price,
    count,
    value_score,
    ROW_NUMBER() OVER (ORDER BY value_score DESC) AS rank
FROM scored
ORDER BY rank
