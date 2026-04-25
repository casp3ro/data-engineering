from unittest.mock import patch

from src.infrastructure.kafka.producer import ListingProducer


def test_send_batch_returns_correct_count() -> None:
    with patch("src.infrastructure.kafka.producer.KafkaProducer"):
        producer = ListingProducer()
        rows = [{"id": str(i), "make": "toyota"} for i in range(10)]
        assert producer.send_batch(iter(rows)) == 10


def test_send_calls_kafka_send() -> None:
    with patch("src.infrastructure.kafka.producer.KafkaProducer") as mock_kafka:
        producer = ListingProducer()
        producer.send({"id": "abc", "make": "ford"})
        mock_kafka.return_value.send.assert_called_once()
