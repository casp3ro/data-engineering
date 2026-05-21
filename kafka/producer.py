"""
Kafka producer — reads a vehicles CSV, validates each row via the domain Listing model,
serialises to Avro, and publishes to the car-listings topic.

Role in pipeline:  CSV on disk  →  Kafka topic  →  Databricks bronze ingest
"""
from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import fastavro
from confluent_kafka import Producer

from kafka.config import KafkaConfig

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent / "schemas" / "listing.avsc"


def _load_schema() -> dict:
    with _SCHEMA_PATH.open() as f:
        return fastavro.parse_schema(json.load(f))


@dataclass
class ProduceResult:
    success_count: int = 0
    error_count: int = 0
    errors: list[str] = field(default_factory=list)

    def __repr__(self) -> str:
        return f"ProduceResult(ok={self.success_count}, err={self.error_count})"


class ListingProducer:
    """
    Produces validated vehicle listings to Kafka as Avro-encoded messages.

    Uses the listing id as message key for idempotency (same id → same partition).
    Invalid rows are skipped with a warning — one bad row does not abort the batch.
    """

    def __init__(self, config: KafkaConfig | None = None) -> None:
        self._config = config or KafkaConfig()
        self._schema = _load_schema()
        self._producer = Producer(self._config.producer_conf)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def produce_from_csv(self, path: Path) -> ProduceResult:
        import csv

        result = ProduceResult()
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            batch: list[dict] = []
            for row in reader:
                record = self._parse_row(row, result)
                if record is None:
                    continue
                batch.append(record)
                if len(batch) >= self._config.batch_size:
                    self._flush_batch(batch, result)
                    batch = []
            if batch:
                self._flush_batch(batch, result)
        return result

    def produce_single(self, record: dict) -> None:
        """Produce one already-validated record dict."""
        self._send(record)
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _parse_row(self, row: dict, result: ProduceResult) -> dict | None:
        from src.domain.constants import (
            MAX_MILEAGE,
            MAX_PRICE,
            MAX_YEAR,
            MIN_MILEAGE,
            MIN_PRICE,
            MIN_YEAR,
        )

        row_id = row.get("id", "<unknown>")
        try:
            price = float(row["price"])
            year = int(row["year"])
            mileage_raw = row.get("odometer") or row.get("mileage")
            mileage: float | None = float(mileage_raw) if mileage_raw else None

            if not (MIN_PRICE <= price <= MAX_PRICE):
                raise ValueError(f"price {price} out of range")
            if not (MIN_YEAR <= year <= MAX_YEAR):
                raise ValueError(f"year {year} out of range")
            if mileage is not None and not (MIN_MILEAGE <= mileage <= MAX_MILEAGE):
                raise ValueError(f"mileage {mileage} out of range")

            return {
                "id": str(row_id),
                "price": price,
                "year": year,
                "manufacturer": (row.get("manufacturer") or "").lower().strip(),
                "model": (row.get("model") or "").lower().strip(),
                "condition": (row.get("condition") or "").lower().strip(),
                "fuel": (row.get("fuel") or "").lower().strip(),
                "mileage": mileage,
                "transmission": (row.get("transmission") or "").lower().strip(),
                "drive": (row.get("drive") or "").lower().strip(),
                "state": (row.get("state") or "").lower().strip(),
                "lat": float(row["lat"]) if row.get("lat") else None,
                "long": float(row["long"]) if row.get("long") else None,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            result.error_count += 1
            result.errors.append(f"row id={row_id}: {e}")
            logger.warning("Skipping row id=%s: %s", row_id, e)
            return None

    def _flush_batch(self, batch: list[dict], result: ProduceResult) -> None:
        for record in batch:
            self._send(record)
        self._producer.flush()
        result.success_count += len(batch)
        logger.info("Flushed batch of %d records", len(batch))

    def _send(self, record: dict) -> None:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self._schema, record)
        self._producer.produce(
            topic=self._config.topic,
            key=record["id"].encode(),
            value=buf.getvalue(),
        )
