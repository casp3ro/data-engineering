#!/usr/bin/env python
"""Produce vehicle listings from CSV to Kafka."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka.config import KafkaConfig
from kafka.producer import ListingProducer


def main() -> None:
    csv_path = Path("data/raw/vehicles.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    producer = ListingProducer(KafkaConfig())
    result = producer.produce_from_csv(csv_path)
    producer.close()
    print(result)


if __name__ == "__main__":
    main()
