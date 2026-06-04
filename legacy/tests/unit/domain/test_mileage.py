import pytest

from src.domain.value_objects.mileage import Mileage


@pytest.mark.parametrize("value", [0, 50_000, 499_999])
def test_valid_mileage(value: int) -> None:
    assert Mileage(value=value).value == value


@pytest.mark.parametrize("value", [-1, 1_000_001])
def test_invalid_mileage(value: int) -> None:
    with pytest.raises(Exception):
        Mileage(value=value)
