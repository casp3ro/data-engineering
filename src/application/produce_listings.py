from pathlib import Path

import polars as pl

from src.domain.entities.listing import Listing
from src.domain.value_objects.mileage import Mileage
from src.domain.value_objects.price import Price
from src.infrastructure.kafka.producer import ListingProducer

BATCH_SIZE = 1_000


class ProduceListings:
    def __init__(self, producer: ListingProducer) -> None:
        self._producer = producer

    def execute(self, csv_path: Path) -> dict:
        total, failed = 0, 0
        df = pl.read_csv(csv_path, infer_schema_length=10_000)

        for i in range(0, len(df), BATCH_SIZE):
            batch = df.slice(i, BATCH_SIZE)
            valid, errors = self._parse_batch(batch)
            self._producer.send_batch(iter(valid))
            total += len(valid)
            failed += errors

        return {"produced": total, "failed": failed}

    def _parse_batch(self, batch: pl.DataFrame) -> tuple[list[dict], int]:
        valid: list[dict] = []
        errors = 0
        for row in batch.iter_rows(named=True):
            try:
                listing = self._parse_row(row)
                valid.append(listing.to_dict())
            except Exception:
                errors += 1
        return valid, errors

    def _parse_row(self, row: dict) -> Listing:
        return Listing(
            id=str(row["id"]),
            make=str(row.get("manufacturer", "")).lower().strip(),
            model=str(row.get("model", "")).lower().strip(),
            year=int(row["year"]),
            price=Price(amount=float(row["price"])),
            mileage=Mileage(value=int(row.get("odometer", 0))),
            state=str(row.get("state", "")),
            condition=row.get("condition"),
        )
