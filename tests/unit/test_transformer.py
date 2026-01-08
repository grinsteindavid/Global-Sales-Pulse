from datetime import datetime, timezone
from decimal import Decimal

import pytest

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "etl", "src"))

from transformations.transformer import TransactionTransformer


class TestTransactionTransformer:
    @pytest.fixture
    def transformer(self):
        return TransactionTransformer(batch_id="test_batch_001")

    @pytest.fixture
    def valid_raw_message(self):
        return {
            "order_id": "test-order-123",
            "amount": "150.00",
            "currency": "USD",
            "timestamp": "2024-01-15T10:30:00Z",
            "customer_id": "cust_12345",
            "product_category": "electronics",
        }

    def test_transform_single_valid(self, transformer, valid_raw_message):
        result = transformer.transform_single(valid_raw_message)

        assert result is not None
        assert result.order_id == "test-order-123"
        assert result.amount == Decimal("150.00")
        assert result.currency == "USD"
        assert result.tax_amount == Decimal("12.75")
        assert result.total_amount == Decimal("162.75")
        assert result.is_fraud_flagged is False
        assert result.batch_id == "test_batch_001"

    def test_transform_single_fraud_flagged(self, transformer):
        raw = {
            "order_id": "fraud-order",
            "amount": "1500.00",
            "currency": "USD",
            "timestamp": "2024-01-15T10:30:00Z",
            "customer_id": "cust_99999",
            "product_category": "electronics",
        }
        result = transformer.transform_single(raw)

        assert result is not None
        assert result.is_fraud_flagged is True

    def test_transform_single_invalid_returns_none(self, transformer):
        invalid_raw = {
            "order_id": "invalid",
        }
        result = transformer.transform_single(invalid_raw)
        assert result is None

    def test_transform_batch(self, transformer, valid_raw_message):
        raw_messages = [valid_raw_message, valid_raw_message.copy()]
        raw_messages[1]["order_id"] = "test-order-456"

        transformed, failed = transformer.transform_batch(raw_messages)

        assert len(transformed) == 2
        assert len(failed) == 0

    def test_transform_batch_with_failures(self, transformer, valid_raw_message):
        raw_messages = [
            valid_raw_message,
            {"invalid": "message"},
        ]

        transformed, failed = transformer.transform_batch(raw_messages)

        assert len(transformed) == 1
        assert len(failed) == 1
