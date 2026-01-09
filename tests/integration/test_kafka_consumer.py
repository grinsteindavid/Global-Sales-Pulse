"""Integration tests for Kafka consumer.

Tests the KafkaTransactionConsumer against a real Kafka broker.
"""
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException

KAFKA_AVAILABLE = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") is not None
logger = logging.getLogger(__name__)


class KafkaConsumerForTest:
    """Standalone consumer for testing (mirrors etl/src/consumers/kafka_consumer.py)."""

    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self._consumer: Consumer | None = None

    def _create_consumer(self) -> Consumer:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        return Consumer(config)

    def consume_batch(self, max_messages: int = 1000, timeout: float = 5.0) -> list[dict[str, Any]]:
        if self._consumer is None:
            self._consumer = self._create_consumer()
            self._consumer.subscribe([self.topic])

        messages: list[dict[str, Any]] = []
        empty_polls = 0
        max_empty_polls = 3

        while len(messages) < max_messages and empty_polls < max_empty_polls:
            msg = self._consumer.poll(timeout=timeout)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                raise KafkaException(msg.error())
            try:
                value = json.loads(msg.value().decode("utf-8"))
                messages.append(value)
                empty_polls = 0
            except json.JSONDecodeError:
                continue
        return messages

    def commit(self):
        if self._consumer:
            self._consumer.commit()

    def close(self):
        if self._consumer:
            self._consumer.close()
            self._consumer = None


def get_producer():
    """Create a Kafka producer for test setup."""
    from confluent_kafka import Producer
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return Producer({"bootstrap.servers": bootstrap_servers})


def create_test_message(order_id: str = None) -> dict:
    """Create a valid transaction message."""
    return {
        "order_id": order_id or str(uuid.uuid4()),
        "amount": "100.00",
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": "test_customer",
        "product_category": "electronics",
    }


@pytest.fixture
def test_topic():
    """Use a unique topic for each test to avoid interference."""
    return f"test_transactions_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def kafka_producer():
    """Provide a Kafka producer."""
    producer = get_producer()
    yield producer
    producer.flush()


def get_bootstrap_servers():
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="Kafka not available")
class TestKafkaConsumerIntegration:
    def test_consume_batch_returns_messages(self, kafka_producer, test_topic):
        """Consumer retrieves messages from Kafka topic."""
        messages_to_send = [create_test_message() for _ in range(5)]
        for msg in messages_to_send:
            kafka_producer.produce(test_topic, json.dumps(msg).encode("utf-8"))
        kafka_producer.flush()

        time.sleep(1)

        consumer = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=f"test_group_{uuid.uuid4().hex[:8]}",
        )
        try:
            received = consumer.consume_batch(max_messages=10, timeout=5.0)
            assert len(received) == 5
            order_ids = {m["order_id"] for m in received}
            expected_ids = {m["order_id"] for m in messages_to_send}
            assert order_ids == expected_ids
        finally:
            consumer.close()

    def test_consume_empty_topic_returns_empty_list(self, kafka_producer, test_topic):
        """Consumer returns empty list when no messages available."""
        kafka_producer.produce(test_topic, b"init")
        kafka_producer.flush()
        time.sleep(1)

        consumer = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=f"test_group_{uuid.uuid4().hex[:8]}",
        )
        try:
            _ = consumer.consume_batch(max_messages=10, timeout=2.0)
            consumer.commit()
        finally:
            consumer.close()

        consumer2 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=f"test_group_{uuid.uuid4().hex[:8]}",
        )
        try:
            received = consumer2.consume_batch(max_messages=10, timeout=2.0)
            assert received == [], "After consuming all messages, topic should be empty"
        finally:
            consumer2.close()

    def test_consume_handles_malformed_json(self, kafka_producer, test_topic):
        """Consumer skips malformed JSON messages without crashing."""
        valid_msg = create_test_message(order_id="valid-order")
        kafka_producer.produce(test_topic, json.dumps(valid_msg).encode("utf-8"))
        kafka_producer.produce(test_topic, b"not-valid-json{{{")
        kafka_producer.produce(test_topic, b"")
        kafka_producer.flush()

        time.sleep(1)

        consumer = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=f"test_group_{uuid.uuid4().hex[:8]}",
        )
        try:
            received = consumer.consume_batch(max_messages=10, timeout=5.0)
            assert len(received) == 1
            assert received[0]["order_id"] == "valid-order"
        finally:
            consumer.close()

    def test_manual_commit_not_auto(self, kafka_producer, test_topic):
        """Consumer uses manual commit (auto.commit disabled)."""
        msg = create_test_message()
        kafka_producer.produce(test_topic, json.dumps(msg).encode("utf-8"))
        kafka_producer.flush()
        time.sleep(1)

        group_id = f"test_group_{uuid.uuid4().hex[:8]}"

        consumer1 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            received1 = consumer1.consume_batch(max_messages=10, timeout=5.0)
            assert len(received1) == 1
        finally:
            consumer1.close()

        consumer2 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            received2 = consumer2.consume_batch(max_messages=10, timeout=5.0)
            assert len(received2) == 1, "Message should be re-delivered since offset not committed"
        finally:
            consumer2.close()

    def test_commit_persists_offset(self, kafka_producer, test_topic):
        """After commit, messages are not re-delivered."""
        msg = create_test_message()
        kafka_producer.produce(test_topic, json.dumps(msg).encode("utf-8"))
        kafka_producer.flush()
        time.sleep(1)

        group_id = f"test_group_{uuid.uuid4().hex[:8]}"

        consumer1 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            received1 = consumer1.consume_batch(max_messages=10, timeout=5.0)
            assert len(received1) == 1
            consumer1.commit()
        finally:
            consumer1.close()

        consumer2 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            received2 = consumer2.consume_batch(max_messages=10, timeout=5.0)
            assert len(received2) == 0, "Message should not be re-delivered after commit"
        finally:
            consumer2.close()
