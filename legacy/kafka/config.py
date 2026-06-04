"""
Kafka configuration — reads all connection settings from env vars.
Role: central config for both producer and consumer in the pipeline.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC", "car-listings")
    )
    schema_registry_url: str = field(
        default_factory=lambda: os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    )
    batch_size: int = 500

    def __post_init__(self) -> None:
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be empty")
        if not self.topic:
            raise ValueError("KAFKA_TOPIC must not be empty")

    @property
    def producer_conf(self) -> dict[str, str]:
        return {"bootstrap.servers": self.bootstrap_servers}
