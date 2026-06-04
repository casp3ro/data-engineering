import pytest

from src.domain.value_objects.price import Price


@pytest.mark.parametrize("amount", [500.0, 12_000.0, 199_999.0])
def test_valid_prices(amount: float) -> None:
    assert Price(amount=amount).amount == amount


@pytest.mark.parametrize("amount", [0.0, -100.0, 600_000.0])
def test_invalid_prices(amount: float) -> None:
    with pytest.raises(Exception):
        Price(amount=amount)
