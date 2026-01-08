"""Unit tests for Producer models."""
import json
import importlib.util
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

producer_models_path = Path(__file__).parent.parent.parent / "producer" / "src" / "models.py"
PRODUCER_AVAILABLE = producer_models_path.exists()


def load_producer_transaction():
    """Load Transaction from producer module directly to avoid path conflicts."""
    if not PRODUCER_AVAILABLE:
        return None
    spec = importlib.util.spec_from_file_location("producer_models", producer_models_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Transaction


ProducerTransaction = load_producer_transaction()

pytestmark = pytest.mark.skipif(
    not PRODUCER_AVAILABLE,
    reason="Producer code not available in this environment"
)


class TestProducerTransaction:
    def test_auto_generates_order_id(self):
        tx = ProducerTransaction(
            amount=Decimal("100.00"),
            customer_id="cust_001",
            product_category="electronics",
        )
        assert tx.order_id is not None
        assert len(tx.order_id) == 36  # UUID format

    def test_auto_generates_timestamp(self):
        tx = ProducerTransaction(
            amount=Decimal("100.00"),
            customer_id="cust_001",
            product_category="electronics",
        )
        assert tx.timestamp is not None
        assert tx.timestamp.tzinfo is not None

    def test_default_currency_is_usd(self):
        tx = ProducerTransaction(
            amount=Decimal("100.00"),
            customer_id="cust_001",
            product_category="electronics",
        )
        assert tx.currency == "USD"

    def test_custom_currency(self):
        tx = ProducerTransaction(
            amount=Decimal("100.00"),
            currency="EUR",
            customer_id="cust_001",
            product_category="electronics",
        )
        assert tx.currency == "EUR"

    def test_to_json_returns_valid_json(self):
        tx = ProducerTransaction(
            amount=Decimal("150.50"),
            customer_id="cust_002",
            product_category="clothing",
        )
        json_str = tx.to_json()
        parsed = json.loads(json_str)
        
        assert "order_id" in parsed
        assert "amount" in parsed
        assert "currency" in parsed
        assert "timestamp" in parsed
        assert "customer_id" in parsed
        assert "product_category" in parsed

    def test_amount_validation_non_negative(self):
        tx = ProducerTransaction(
            amount=Decimal("0.00"),
            customer_id="cust_001",
            product_category="electronics",
        )
        assert tx.amount >= 0

    def test_amount_decimal_precision(self):
        tx = ProducerTransaction(
            amount=Decimal("99.99"),
            customer_id="cust_001",
            product_category="electronics",
        )
        assert str(tx.amount) == "99.99"
