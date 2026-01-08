import json
import logging
import random
import time
from decimal import Decimal

from confluent_kafka import Producer

from .config import settings
from .models import Transaction

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PRODUCT_CATEGORIES = [
    "electronics",
    "clothing",
    "home_garden",
    "sports",
    "books",
    "toys",
    "automotive",
    "health_beauty",
]

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]


def delivery_callback(err, msg):
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def generate_transaction() -> Transaction:
    return Transaction(
        amount=Decimal(str(round(random.uniform(10.0, 2000.0), 2))),
        currency=random.choice(CURRENCIES),
        customer_id=f"cust_{random.randint(10000, 99999)}",
        product_category=random.choice(PRODUCT_CATEGORIES),
    )


def create_producer() -> Producer:
    config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "transaction-producer",
        "acks": "all",
    }
    return Producer(config)


def run():
    logger.info(f"Starting producer, connecting to {settings.kafka_bootstrap_servers}")
    producer = create_producer()

    try:
        while True:
            transaction = generate_transaction()
            message = transaction.to_json()

            producer.produce(
                topic=settings.kafka_topic_transactions,
                value=message.encode("utf-8"),
                callback=delivery_callback,
            )
            producer.poll(0)

            logger.info(
                f"Produced: order_id={transaction.order_id}, "
                f"amount={transaction.amount}, "
                f"category={transaction.product_category}"
            )

            interval = random.uniform(
                settings.producer_interval_min,
                settings.producer_interval_max,
            )
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.flush()
        logger.info("Producer shutdown complete")


if __name__ == "__main__":
    run()
