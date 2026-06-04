-- Read Gold price_by_brand (Databricks path: exported to MinIO by notebook 04)

SELECT
    manufacturer,
    avg_price,
    median_price,
    count,
    min_price,
    max_price
FROM delta_scan('{{ get_delta_path("gold_price_by_brand") }}')
WHERE manufacturer IS NOT NULL
  AND count >= {{ var('min_count') }}
