"""True end-to-end tests for the ETL pipeline.

Tests the full flow: Kafka → Airflow DAG → Warehouse.
These tests inject messages into Kafka and verify they arrive in the database.
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import requests

etl_src = Path(__file__).parent.parent.parent / "etl" / "src"
sys.path.insert(0, str(etl_src))

from models.transaction import Transaction


KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "transactions_e2e_test")
AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.environ.get("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")


def create_test_message(
    order_id: str = None,
    amount: float = 100.00,
    currency: str = "USD",
    customer_id: str = "cust_12345",
    product_category: str = "electronics",
) -> dict:
    """Create a test transaction message."""
    return {
        "order_id": order_id or str(uuid.uuid4()),
        "amount": str(amount),
        "currency": currency,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer_id,
        "product_category": product_category,
    }


def trigger_airflow_dag(dag_id: str = "etl_pipeline") -> bool:
    """Trigger an Airflow DAG via REST API."""
    import logging
    logger = logging.getLogger(__name__)
    
    url = f"{AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
    logger.info(f"Triggering DAG {dag_id} at {url}")
    
    try:
        response = requests.post(
            url,
            json={"conf": {}},
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=10,
        )
        success = response.status_code in (200, 201)
        if success:
            logger.info(f"DAG {dag_id} triggered successfully")
        else:
            logger.warning(f"DAG trigger failed: {response.status_code} - {response.text}")
        return success
    except requests.RequestException as e:
        logger.error(f"Failed to trigger DAG: {e}")
        return False


def wait_for_record(session, order_id: str, timeout: int = 20) -> Transaction | None:
    """Poll database until record appears or timeout."""
    import logging
    logger = logging.getLogger(__name__)
    
    start = time.time()
    attempt = 0
    while time.time() - start < timeout:
        attempt += 1
        record = session.query(Transaction).filter_by(order_id=order_id).first()
        if record:
            logger.info(f"Found record {order_id} after {attempt} attempts ({time.time() - start:.1f}s)")
            return record
        logger.debug(f"Waiting for {order_id}... attempt {attempt}")
        time.sleep(1)
    
    logger.warning(f"Record {order_id} not found after {timeout}s ({attempt} attempts)")
    return None


@pytest.mark.e2e
class TestFullPipelineE2E:
    """True E2E tests: Kafka → Airflow → Warehouse."""

    def test_message_flows_kafka_to_warehouse(self, kafka_producer, session):
        """Inject message to Kafka, trigger DAG, verify in warehouse."""
        import logging
        logger = logging.getLogger(__name__)
        
        order_id = f"e2e-full-{uuid.uuid4().hex[:8]}"
        message = create_test_message(
            order_id=order_id,
            amount=250.00,
            currency="USD",
            product_category="electronics",
        )

        logger.info(f"Producing message {order_id} to Kafka topic {KAFKA_TOPIC}")
        kafka_producer.produce(
            KAFKA_TOPIC,
            json.dumps(message).encode("utf-8"),
        )
        kafka_producer.flush()
        logger.info(f"Message {order_id} flushed to Kafka")

        trigger_airflow_dag("etl_pipeline")

        record = wait_for_record(session, order_id, timeout=60)

        assert record is not None, f"Record {order_id} not found in warehouse"
        assert float(record.amount) == 250.00
        assert record.currency == "USD"
        assert record.tax_amount == Decimal("21.25")
        assert record.total_amount == Decimal("271.25")

    def test_batch_messages_flow(self, kafka_producer, session):
        """Multiple messages flow through the pipeline."""
        batch_prefix = f"e2e-batch-{uuid.uuid4().hex[:6]}"
        order_ids = [f"{batch_prefix}-{i}" for i in range(3)]

        for order_id in order_ids:
            message = create_test_message(order_id=order_id, amount=100.00)
            kafka_producer.produce(KAFKA_TOPIC, json.dumps(message).encode("utf-8"))
        kafka_producer.flush()

        trigger_airflow_dag("etl_pipeline")

        for order_id in order_ids:
            record = wait_for_record(session, order_id, timeout=30)
            assert record is not None, f"Record {order_id} not found"

    def test_fraud_detection_e2e(self, kafka_producer, session):
        """High-value transaction gets fraud-flagged through full pipeline."""
        order_id = f"e2e-fraud-{uuid.uuid4().hex[:8]}"
        message = create_test_message(
            order_id=order_id,
            amount=1500.00,
            customer_id="suspicious_customer",
        )

        kafka_producer.produce(KAFKA_TOPIC, json.dumps(message).encode("utf-8"))
        kafka_producer.flush()

        trigger_airflow_dag("etl_pipeline")

        record = wait_for_record(session, order_id, timeout=30)

        assert record is not None
        assert record.is_fraud_flagged is True

    def test_invalid_message_does_not_break_pipeline(self, kafka_producer, session):
        """Invalid message is skipped, valid message still processed."""
        valid_order_id = f"e2e-valid-{uuid.uuid4().hex[:8]}"

        kafka_producer.produce(
            KAFKA_TOPIC,
            json.dumps({"invalid": "message"}).encode("utf-8"),
        )
        kafka_producer.produce(
            KAFKA_TOPIC,
            json.dumps(create_test_message(order_id=valid_order_id)).encode("utf-8"),
        )
        kafka_producer.flush()

        trigger_airflow_dag("etl_pipeline")

        record = wait_for_record(session, valid_order_id, timeout=90)
        assert record is not None, "Valid message should still be processed"


@pytest.mark.e2e
class TestKafkaMessageDelivery:
    """Tests that verify messages are correctly delivered to Kafka."""

    def test_producer_message_lands_in_kafka(self, kafka_producer, kafka_consumer):
        """Verify a message produced lands in Kafka topic."""
        order_id = f"kafka-test-{uuid.uuid4().hex[:8]}"
        message = create_test_message(order_id=order_id)

        kafka_producer.produce(KAFKA_TOPIC, json.dumps(message).encode("utf-8"))
        kafka_producer.flush()

        consumed = None
        for _ in range(30):
            msg = kafka_consumer.poll(timeout=1.0)
            if msg and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))
                if data.get("order_id") == order_id:
                    consumed = data
                    break

        assert consumed is not None, "Message not found in Kafka"
        assert consumed["order_id"] == order_id
        assert float(consumed["amount"]) == 100.00
