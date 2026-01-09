import json
import logging
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from ..config import settings

logger = logging.getLogger(__name__)


class KafkaTransactionConsumer:
    def __init__(
        self,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        topic: str | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.group_id = group_id or settings.kafka_consumer_group
        self.topic = topic or settings.kafka_topic_transactions
        self._consumer: Consumer | None = None

    def _create_consumer(self) -> Consumer:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        return Consumer(config)

    def consume_batch(
        self,
        max_messages: int = 1000,
        timeout: float = 5.0,
        max_duration: float = 20.0,
    ) -> list[dict[str, Any]]:
        if self._consumer is None:
            self._consumer = self._create_consumer()
            self._consumer.subscribe([self.topic])

        messages: list[dict[str, Any]] = []
        empty_polls = 0
        max_empty_polls = 3
        start_time = time.time()

        while len(messages) < max_messages and empty_polls < max_empty_polls:
            if time.time() - start_time > max_duration:
                logger.info(f"Reached max duration of {max_duration}s, stopping consumption")
                break

            msg = self._consumer.poll(timeout=timeout)

            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"End of partition reached: {msg.topic()}[{msg.partition()}]")
                    empty_polls += 1
                    continue
                raise KafkaException(msg.error())

            try:
                value = json.loads(msg.value().decode("utf-8"))
                messages.append(value)
                empty_polls = 0
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
                continue

        logger.info(f"Consumed {len(messages)} messages from {self.topic}")
        return messages

    def commit(self):
        if self._consumer:
            self._consumer.commit()
            logger.info("Committed offsets")

    def close(self):
        if self._consumer:
            self._consumer.close()
            self._consumer = None
            logger.info("Consumer closed")
