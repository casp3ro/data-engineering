-- Fails if any year in the price_by_year model falls outside declared bounds.
-- Expected: 0 rows returned.

SELECT year, avg_price
FROM {{ ref('stg_price_by_year') }}
WHERE year < {{ var('min_year') }}
   OR year > {{ var('max_year') }}
