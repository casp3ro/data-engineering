from unittest.mock import patch

from src.infrastructure.kafka.producer import ListingProducer


def test_send_batch_returns_correct_count() -> None:
    with patch("src.infrastructure.kafka.producer.Producer"):
        producer = ListingProducer()
        rows = [{"id": str(i), "make": "toyota"} for i in range(10)]
        assert producer.send_batch(iter(rows)) == 10


def test_send_calls_kafka_produce() -> None:
    with patch("src.infrastructure.kafka.producer.Producer") as mock_cls:
        producer = ListingProducer()
        producer.send({"id": "abc", "make": "ford"})
        mock_cls.return_value.produce.assert_called_once()
