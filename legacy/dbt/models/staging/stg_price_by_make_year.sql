SELECT
    make,
    year,
    listing_count,
    median_price
FROM delta_scan('{{ get_delta_path("gold_price_by_make_year") }}')
WHERE make IS NOT NULL
  AND year IS NOT NULL
