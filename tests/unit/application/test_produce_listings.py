import csv
from pathlib import Path
from typing import Iterator

from src.application.ports.listing_producer import IListingProducer
from src.application.produce_listings import ProduceListings


class _FakeProducer:
    def __init__(self) -> None:
        self.batches: list[list[dict]] = []

    def send_batch(self, listings: Iterator[dict]) -> int:
        batch = list(listings)
        self.batches.append(batch)
        return len(batch)


def _write_csv(path: Path, rows: list[dict]) -> None:
    headers = ["id", "manufacturer", "model", "year", "price", "odometer", "state", "condition"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _row(**kwargs: object) -> dict:
    base: dict = {
        "id": "1", "manufacturer": "toyota", "model": "camry",
        "year": 2018, "price": 15000.0, "odometer": 50000,
        "state": "ca", "condition": "good",
    }
    return {**base, **kwargs}


def test_fake_producer_satisfies_protocol() -> None:
    assert isinstance(_FakeProducer(), IListingProducer)


def test_valid_rows_are_produced(tmp_path: Path) -> None:
    _write_csv(tmp_path / "v.csv", [_row(id=str(i)) for i in range(5)])
    producer = _FakeProducer()
    result = ProduceListings(producer).execute(tmp_path / "v.csv")
    assert result == {"produced": 5, "failed": 0}


def test_invalid_price_counted_as_failed(tmp_path: Path) -> None:
    _write_csv(tmp_path / "v.csv", [_row(id="1"), _row(id="2", price=-1)])
    producer = _FakeProducer()
    result = ProduceListings(producer).execute(tmp_path / "v.csv")
    assert result["produced"] == 1
    assert result["failed"] == 1


def test_empty_csv_returns_zeros(tmp_path: Path) -> None:
    (tmp_path / "v.csv").write_text(
        "id,manufacturer,model,year,price,odometer,state,condition\n"
    )
    producer = _FakeProducer()
    result = ProduceListings(producer).execute(tmp_path / "v.csv")
    assert result == {"produced": 0, "failed": 0}


def test_batching_calls_send_batch_per_chunk(tmp_path: Path, monkeypatch: object) -> None:
    import src.application.produce_listings as mod
    monkeypatch.setattr(mod, "BATCH_SIZE", 2)
    _write_csv(tmp_path / "v.csv", [_row(id=str(i)) for i in range(5)])
    producer = _FakeProducer()
    ProduceListings(producer).execute(tmp_path / "v.csv")
    assert len(producer.batches) == 3  # ceil(5 / 2) = 3 batches
