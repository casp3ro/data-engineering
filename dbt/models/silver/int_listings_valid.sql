{{ config(materialized='table', file_format='delta') }}

SELECT *
FROM {{ ref('stg_listings') }}
WHERE
    price   BETWEEN {{ var('min_price') }}   AND {{ var('max_price') }}
    AND year BETWEEN {{ var('min_year') }}   AND {{ var('max_year') }}
    AND mileage BETWEEN {{ var('min_mileage') }} AND {{ var('max_mileage') }}
    AND make NOT IN ('', 'unknown')
    AND state IS NOT NULL
    AND LENGTH(state) = 2
    AND state IN (
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    )
