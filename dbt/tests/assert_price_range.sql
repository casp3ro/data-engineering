-- Fails if any brand has avg_price outside the declared business bounds.
-- Expected: 0 rows returned.

SELECT manufacturer, avg_price
FROM {{ ref('stg_price_by_brand') }}
WHERE avg_price < {{ var('min_price') }}
   OR avg_price > {{ var('max_price') }}
