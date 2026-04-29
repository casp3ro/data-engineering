{{ config(materialized='table', file_format='delta') }}

SELECT *
FROM {{ ref('stg_listings') }}
WHERE
    price   BETWEEN 500   AND 200000
    AND year BETWEEN 1990  AND 2024
    AND mileage BETWEEN 0  AND 500000
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

