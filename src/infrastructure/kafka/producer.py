import json
import logging
from typing import Iterator

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

TOPIC = "car_listings_raw"


class ListingProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def send(self, listing_dict: dict) -> None:
        self._producer.produce(
            topic=TOPIC,
            key=listing_dict["id"].encode(),
            value=json.dumps(listing_dict).encode("utf-8"),
        )

    def send_batch(self, listings: Iterator[dict]) -> int:
        count = 0
        for listing in listings:
            self.send(listing)
            count += 1
        self._producer.flush()
        return count

    def close(self) -> None:
        self._producer.flush()
