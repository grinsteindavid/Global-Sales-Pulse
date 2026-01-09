import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

etl_src_local = Path(__file__).parent.parent / "etl" / "src"
etl_src_docker = Path("/app/etl_src")

if etl_src_local.exists():
    sys.path.insert(0, str(etl_src_local))
elif etl_src_docker.exists():
    sys.path.insert(0, str(etl_src_docker))

from models.base import Base


@pytest.fixture(scope="session")
def database_url():
    return os.environ.get(
        "WAREHOUSE_DATABASE_URL",
        "postgresql+psycopg2://warehouse:warehouse@localhost:5433/warehouse",
    )


@pytest.fixture(scope="session")
def engine(database_url):
    return create_engine(database_url)


@pytest.fixture(scope="session")
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture
def session(engine, tables):
    connection = engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="session")
def kafka_bootstrap_servers():
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


@pytest.fixture
def kafka_producer(kafka_bootstrap_servers):
    """Provide a Kafka producer for tests."""
    try:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
        yield producer
        producer.flush()
    except ImportError:
        pytest.skip("confluent_kafka not available")


@pytest.fixture
def kafka_consumer(kafka_bootstrap_servers, test_topic):
    """Provide a Kafka consumer for tests."""
    try:
        from confluent_kafka import Consumer
        consumer = Consumer({
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": f"test-consumer-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        })
        topic = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", test_topic)
        consumer.subscribe([topic])
        yield consumer
        consumer.close()
    except ImportError:
        pytest.skip("confluent_kafka not available")


@pytest.fixture
def test_topic():
    """Generate a unique topic name for test isolation."""
    return f"test_transactions_{uuid.uuid4().hex[:8]}"


def create_test_message(order_id: str = None) -> dict:
    """Helper to create a valid transaction message for tests."""
    return {
        "order_id": order_id or str(uuid.uuid4()),
        "amount": "100.00",
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": "test_customer",
        "product_category": "electronics",
    }
