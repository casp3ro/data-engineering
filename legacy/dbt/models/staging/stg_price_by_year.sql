SELECT
    year,
    avg_price,
    count
FROM delta_scan('{{ get_delta_path("gold_price_by_year") }}')
WHERE year IS NOT NULL
