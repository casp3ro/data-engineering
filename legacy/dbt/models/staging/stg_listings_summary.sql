SELECT
    total_listings,
    unique_makes,
    states_covered,
    overall_median_price,
    overall_avg_price,
    oldest_year,
    newest_year
FROM delta_scan('{{ get_delta_path("gold_listings_summary") }}')
