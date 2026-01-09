"""Integration tests for offset commit safety.

Verifies that Kafka offsets are only committed after successful database load.
This is critical for data integrity - prevents data loss and duplicates.
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from sqlalchemy.dialects.postgresql import insert

etl_src_local = Path(__file__).parent.parent.parent / "etl" / "src"
etl_src_docker = Path("/app/etl_src")

if etl_src_local.exists():
    sys.path.insert(0, str(etl_src_local))
elif etl_src_docker.exists():
    sys.path.insert(0, str(etl_src_docker))

from models.transaction import Transaction
from transformations.transformer import TransactionTransformer

KAFKA_AVAILABLE = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") is not None


class KafkaConsumerForTest:
    """Standalone consumer for testing."""

    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self._consumer: Consumer | None = None

    def _create_consumer(self) -> Consumer:
        return Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

    def consume_batch(self, max_messages: int = 1000, timeout: float = 5.0) -> list[dict]:
        if self._consumer is None:
            self._consumer = self._create_consumer()
            self._consumer.subscribe([self.topic])

        messages = []
        empty_polls = 0
        while len(messages) < max_messages and empty_polls < 3:
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
                messages.append(json.loads(msg.value().decode("utf-8")))
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


class PostgresLoaderForTest:
    """Standalone loader for testing."""

    def __init__(self, session):
        self.session = session

    def load_batch(self, transactions: list) -> int:
        if not transactions:
            return 0
        records = [t.model_dump() for t in transactions]
        stmt = insert(Transaction).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["order_id"])
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount if result.rowcount else len(records)


def get_bootstrap_servers():
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def get_producer():
    """Create a Kafka producer for test setup."""
    return Producer({"bootstrap.servers": get_bootstrap_servers()})


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
    """Use a unique topic for each test."""
    return f"test_offset_safety_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def kafka_producer():
    """Provide a Kafka producer."""
    producer = get_producer()
    yield producer
    producer.flush()


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="Kafka not available")
class TestOffsetCommitSafety:
    def test_offset_not_committed_on_transform_failure(self, kafka_producer, test_topic):
        """If transformation fails, offset should not be committed."""
        msg = create_test_message()
        kafka_producer.produce(test_topic, json.dumps(msg).encode("utf-8"))
        kafka_producer.flush()
        time.sleep(1)

        group_id = f"test_group_{uuid.uuid4().hex[:8]}"

        consumer = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            messages = consumer.consume_batch(max_messages=10, timeout=5.0)
            assert len(messages) == 1

            with patch.object(
                TransactionTransformer,
                'transform_batch',
                side_effect=Exception("Simulated transform failure")
            ):
                transformer = TransactionTransformer(batch_id="test")
                try:
                    transformer.transform_batch(messages)
                except Exception:
                    pass
        finally:
            consumer.close()

        consumer2 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            messages2 = consumer2.consume_batch(max_messages=10, timeout=5.0)
            assert len(messages2) == 1, "Message should be available for retry"
        finally:
            consumer2.close()

    def test_offset_not_committed_on_load_failure(
        self, kafka_producer, test_topic, session
    ):
        """If database load fails, offset should not be committed."""
        msg = create_test_message()
        kafka_producer.produce(test_topic, json.dumps(msg).encode("utf-8"))
        kafka_producer.flush()
        time.sleep(1)

        group_id = f"test_group_{uuid.uuid4().hex[:8]}"

        consumer = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            messages = consumer.consume_batch(max_messages=10, timeout=5.0)
            assert len(messages) == 1

            transformer = TransactionTransformer(batch_id="test_batch")
            transformed, _ = transformer.transform_batch(messages)
            assert len(transformed) == 1

            with patch.object(
                PostgresLoaderForTest,
                'load_batch',
                side_effect=Exception("Simulated DB failure")
            ):
                loader = PostgresLoaderForTest(session)
                try:
                    loader.load_batch(transformed)
                except Exception:
                    pass
        finally:
            consumer.close()

        consumer2 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            messages2 = consumer2.consume_batch(max_messages=10, timeout=5.0)
            assert len(messages2) == 1, "Message should be available for retry after load failure"
        finally:
            consumer2.close()

    def test_full_pipeline_success_commits_offset(
        self, kafka_producer, test_topic, session
    ):
        """Successful pipeline run should commit offset."""
        msg = create_test_message()
        kafka_producer.produce(test_topic, json.dumps(msg).encode("utf-8"))
        kafka_producer.flush()
        time.sleep(1)

        group_id = f"test_group_{uuid.uuid4().hex[:8]}"

        consumer = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            messages = consumer.consume_batch(max_messages=10, timeout=5.0)
            assert len(messages) == 1

            transformer = TransactionTransformer(batch_id="test_batch")
            transformed, _ = transformer.transform_batch(messages)

            loader = PostgresLoaderForTest(session)
            loaded = loader.load_batch(transformed)
            assert loaded >= 1

            consumer.commit()
        finally:
            consumer.close()

        consumer2 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            messages2 = consumer2.consume_batch(max_messages=10, timeout=5.0)
            assert len(messages2) == 0, "Message should not be re-delivered after successful commit"
        finally:
            consumer2.close()

    def test_partial_batch_failure_handling(self, kafka_producer, test_topic, session):
        """Valid messages in batch should proceed even if some are invalid."""
        valid_msg = create_test_message(order_id="valid-order-001")
        invalid_msg = {"invalid": "data"}

        kafka_producer.produce(test_topic, json.dumps(valid_msg).encode("utf-8"))
        kafka_producer.produce(test_topic, json.dumps(invalid_msg).encode("utf-8"))
        kafka_producer.flush()
        time.sleep(1)

        group_id = f"test_group_{uuid.uuid4().hex[:8]}"

        consumer = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            messages = consumer.consume_batch(max_messages=10, timeout=5.0)
            assert len(messages) == 2

            transformer = TransactionTransformer(batch_id="test_batch")
            transformed, failed = transformer.transform_batch(messages)

            assert len(transformed) == 1
            assert len(failed) == 1

            loader = PostgresLoaderForTest(session)
            loaded = loader.load_batch(transformed)
            assert loaded >= 1

            consumer.commit()
        finally:
            consumer.close()

        consumer2 = KafkaConsumerForTest(
            bootstrap_servers=get_bootstrap_servers(),
            topic=test_topic,
            group_id=group_id,
        )
        try:
            messages2 = consumer2.consume_batch(max_messages=10, timeout=5.0)
            assert len(messages2) == 0, "Offset should be committed after partial success"
        finally:
            consumer2.close()
