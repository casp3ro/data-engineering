SELECT
    state,
    listing_count,
    median_price,
    avg_price
FROM delta_scan('{{ get_delta_path("gold_price_by_state") }}')
WHERE state IS NOT NULL
