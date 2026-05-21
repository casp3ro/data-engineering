"""
Integration smoke test: produce 10 test listings to Kafka and verify the topic has messages.
Requires: KAFKA_BOOTSTRAP_SERVERS env var + a running Kafka broker.
Skipped automatically when KAFKA_BOOTSTRAP_SERVERS is not set.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import pytest

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
TOPIC = os.getenv("KAFKA_TOPIC", "car-listings-smoke-test")


@pytest.mark.integration
@pytest.mark.skipif(not KAFKA_BOOTSTRAP_SERVERS, reason="KAFKA_BOOTSTRAP_SERVERS not set")
def test_produce_and_consume_smoke() -> None:
    from confluent_kafka import Consumer

    from kafka.config import KafkaConfig
    from kafka.producer import ListingProducer

    # Build 10 synthetic records via the producer's internal _parse_row
    cfg = KafkaConfig(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=TOPIC)
    producer = ListingProducer(cfg)

    test_records = [
        {
            "id": f"smoke-{i}",
            "price": 10_000.0 + i * 500,
            "year": 2010 + i,
            "manufacturer": "toyota",
            "model": "camry",
            "condition": "good",
            "fuel": "gas",
            "mileage": 50_000.0,
            "transmission": "automatic",
            "drive": "fwd",
            "state": "ca",
            "lat": 37.0,
            "long": -122.0,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        for i in range(10)
    ]

    for rec in test_records:
        producer.produce_single(rec)
    producer.close()

    # Give broker a moment to commit
    time.sleep(2)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "smoke-test-consumer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    messages_found = 0
    timeout = 10.0
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        messages_found += 1
        if messages_found >= 10:
            break

    consumer.close()
    assert messages_found >= 10, f"Expected 10 messages, found {messages_found}"
