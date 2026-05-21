"""Unit tests for kafka/producer.py — no real Kafka broker required."""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kafka.config import KafkaConfig


# ── helpers ──────────────────────────────────────────────────────────────


def _make_csv(tmp_path: Path, rows: list[dict]) -> Path:
    if not rows:
        p = tmp_path / "empty.csv"
        p.write_text("id,price,year,manufacturer,model,condition,fuel,odometer,transmission,drive,state,lat,long\n")
        return p
    p = tmp_path / "listings.csv"
    with p.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return p


def _valid_row(**overrides: object) -> dict:
    base = {
        "id": "abc123",
        "price": "12000",
        "year": "2015",
        "manufacturer": "toyota",
        "model": "camry",
        "condition": "good",
        "fuel": "gas",
        "odometer": "80000",
        "transmission": "automatic",
        "drive": "fwd",
        "state": "ca",
        "lat": "37.7",
        "long": "-122.4",
    }
    base.update(overrides)
    return base


# ── KafkaConfig tests ─────────────────────────────────────────────────


def test_kafka_config_defaults() -> None:
    cfg = KafkaConfig(bootstrap_servers="localhost:9092", topic="test")
    assert cfg.bootstrap_servers == "localhost:9092"
    assert cfg.topic == "test"
    assert cfg.batch_size == 500


def test_kafka_config_raises_on_empty_servers() -> None:
    with pytest.raises(ValueError, match="KAFKA_BOOTSTRAP_SERVERS"):
        KafkaConfig(bootstrap_servers="", topic="t")


def test_kafka_config_raises_on_empty_topic() -> None:
    with pytest.raises(ValueError, match="KAFKA_TOPIC"):
        KafkaConfig(bootstrap_servers="localhost:9092", topic="")


# ── ListingProducer tests ─────────────────────────────────────────────


@pytest.fixture()
def mock_confluent_producer() -> MagicMock:
    with patch("confluent_kafka.Producer") as mock_cls:
        mock_inst = MagicMock()
        mock_cls.return_value = mock_inst
        yield mock_inst


def test_valid_listing_is_produced(tmp_path: Path, mock_confluent_producer: MagicMock) -> None:
    from kafka.producer import ListingProducer

    cfg = KafkaConfig(bootstrap_servers="localhost:9092", topic="car-listings")
    p = ListingProducer(cfg)
    csv_path = _make_csv(tmp_path, [_valid_row()])
    result = p.produce_from_csv(csv_path)

    assert result.success_count == 1
    assert result.error_count == 0
    mock_confluent_producer.produce.assert_called_once()


def test_invalid_price_row_is_skipped(tmp_path: Path, mock_confluent_producer: MagicMock) -> None:
    from kafka.producer import ListingProducer

    cfg = KafkaConfig(bootstrap_servers="localhost:9092", topic="car-listings")
    p = ListingProducer(cfg)
    csv_path = _make_csv(tmp_path, [_valid_row(price="999999999")])
    result = p.produce_from_csv(csv_path)

    assert result.success_count == 0
    assert result.error_count == 1


def test_invalid_year_row_is_skipped(tmp_path: Path, mock_confluent_producer: MagicMock) -> None:
    from kafka.producer import ListingProducer

    cfg = KafkaConfig(bootstrap_servers="localhost:9092", topic="car-listings")
    p = ListingProducer(cfg)
    csv_path = _make_csv(tmp_path, [_valid_row(year="1800")])
    result = p.produce_from_csv(csv_path)

    assert result.success_count == 0
    assert result.error_count == 1


def test_mixed_valid_invalid_rows(tmp_path: Path, mock_confluent_producer: MagicMock) -> None:
    from kafka.producer import ListingProducer

    cfg = KafkaConfig(bootstrap_servers="localhost:9092", topic="car-listings")
    p = ListingProducer(cfg)
    rows = [
        _valid_row(id="1"),
        _valid_row(id="2", price="0"),   # invalid — below MIN_PRICE
        _valid_row(id="3"),
    ]
    csv_path = _make_csv(tmp_path, rows)
    result = p.produce_from_csv(csv_path)

    assert result.success_count == 2
    assert result.error_count == 1


def test_empty_csv_returns_zero_counts(tmp_path: Path, mock_confluent_producer: MagicMock) -> None:
    from kafka.producer import ListingProducer

    cfg = KafkaConfig(bootstrap_servers="localhost:9092", topic="car-listings")
    p = ListingProducer(cfg)
    csv_path = _make_csv(tmp_path, [])
    result = p.produce_from_csv(csv_path)

    assert result.success_count == 0
    assert result.error_count == 0


def test_produce_result_repr() -> None:
    from kafka.producer import ProduceResult

    r = ProduceResult(success_count=42, error_count=3)
    assert "42" in repr(r)
    assert "3" in repr(r)
