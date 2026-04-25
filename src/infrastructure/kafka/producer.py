import json
from typing import Iterator

from kafka import KafkaProducer

TOPIC = "car_listings_raw"


class ListingProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )

    def send(self, listing_dict: dict) -> None:
        self._producer.send(
            topic=TOPIC,
            key=listing_dict["id"],
            value=listing_dict,
        )

    def send_batch(self, listings: Iterator[dict]) -> int:
        count = 0
        for listing in listings:
            self.send(listing)
            count += 1
        self._producer.flush()
        return count

    def close(self) -> None:
        self._producer.close()
