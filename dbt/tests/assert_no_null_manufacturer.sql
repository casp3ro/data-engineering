-- Fails if any row in the brand ranking mart has a NULL manufacturer.
-- Expected: 0 rows returned.

SELECT manufacturer
FROM {{ ref('mart_brand_ranking') }}
WHERE manufacturer IS NULL OR manufacturer = ''
