from src.domain.entities.listing import Listing
from src.domain.value_objects.mileage import Mileage
from src.domain.value_objects.price import Price


def make_listing(**kwargs) -> Listing:
    defaults = dict(
        id="test-1",
        make="toyota",
        model="camry",
        year=2018,
        price=Price(amount=12_000.0),
        mileage=Mileage(value=80_000),
        state="ca",
    )
    return Listing(**{**defaults, **kwargs})


def test_valid_year() -> None:
    assert make_listing(year=2018).is_valid_year()


def test_invalid_year() -> None:
    assert not make_listing(year=1800).is_valid_year()


def test_price_per_mile() -> None:
    assert make_listing(price=Price(amount=10_000.0), mileage=Mileage(value=100_000)).price_per_mile() == 0.1


def test_zero_mileage_returns_none() -> None:
    assert make_listing(mileage=Mileage(value=0)).price_per_mile() is None


def test_to_dict_has_all_keys() -> None:
    keys = make_listing().to_dict().keys()
    assert set(keys) == {"id", "make", "model", "year", "price", "mileage", "state", "condition"}
