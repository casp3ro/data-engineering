SELECT
    condition,
    manufacturer,
    avg_price,
    count
FROM delta_scan('{{ get_delta_path("gold_price_by_condition") }}')
WHERE condition IS NOT NULL
