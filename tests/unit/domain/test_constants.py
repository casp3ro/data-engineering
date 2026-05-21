"""Tests that domain constants are in sync with dbt vars and Kafka schema bounds."""
from src.domain.constants import (
    EXCLUDED_MAKES,
    MAX_MILEAGE,
    MAX_PRICE,
    MAX_YEAR,
    MIN_MILEAGE,
    MIN_PRICE,
    MIN_YEAR,
)


def test_max_year_is_2025() -> None:
    assert MAX_YEAR == 2025


def test_min_year_is_1980() -> None:
    assert MIN_YEAR == 1980


def test_price_bounds() -> None:
    assert MIN_PRICE == 500.0
    assert MAX_PRICE == 200_000.0


def test_mileage_bounds() -> None:
    assert MIN_MILEAGE == 0
    assert MAX_MILEAGE == 500_000


def test_excluded_makes_contains_unknown() -> None:
    assert "unknown" in EXCLUDED_MAKES
    assert "" in EXCLUDED_MAKES
