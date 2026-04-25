SELECT *
FROM {{ ref('int_listings_valid') }}
WHERE price <= 0
