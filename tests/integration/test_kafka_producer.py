import os

import pytest

_KAFKA_AVAILABLE = bool(os.getenv("KAFKA_BOOTSTRAP_SERVERS"))


@pytest.mark.integration
@pytest.mark.skipif(not _KAFKA_AVAILABLE, reason="KAFKA_BOOTSTRAP_SERVERS not set")
def test_producer_connects_and_sends() -> None:
    from src.infrastructure.kafka.producer import ListingProducer

    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    producer = ListingProducer(bootstrap_servers=bootstrap)
    count = producer.send_batch(iter([{"id": "integration-test", "make": "toyota"}]))
    producer.close()
    assert count == 1
