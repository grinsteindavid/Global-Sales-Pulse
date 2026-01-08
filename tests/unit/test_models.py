"""Unit tests for Pydantic models (RawTransaction, TransformedTransaction)."""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "etl", "src"))

from transformations.transformer import RawTransaction, TransformedTransaction


class TestRawTransaction:
    def test_valid_raw_transaction(self):
        raw = RawTransaction(
            order_id="order-123",
            amount=Decimal("99.99"),
            currency="USD",
            timestamp=datetime.now(timezone.utc),
            customer_id="cust_001",
            product_category="electronics",
        )
        assert raw.order_id == "order-123"
        assert raw.amount == Decimal("99.99")

    def test_timestamp_parsing_iso_format(self):
        raw = RawTransaction(
            order_id="order-123",
            amount=Decimal("50.00"),
            currency="EUR",
            timestamp="2024-01-15T10:30:00Z",
            customer_id="cust_001",
            product_category="books",
        )
        assert raw.timestamp.tzinfo is not None

    def test_timestamp_parsing_with_offset(self):
        raw = RawTransaction(
            order_id="order-123",
            amount=Decimal("50.00"),
            currency="EUR",
            timestamp="2024-01-15T10:30:00+05:00",
            customer_id="cust_001",
            product_category="books",
        )
        assert raw.timestamp.hour == 10

    def test_currency_max_length(self):
        raw = RawTransaction(
            order_id="order-123",
            amount=Decimal("50.00"),
            currency="USD",
            timestamp=datetime.now(timezone.utc),
            customer_id="cust_001",
            product_category="books",
        )
        assert len(raw.currency) <= 3

    def test_missing_required_field_raises(self):
        with pytest.raises(Exception):
            RawTransaction(
                order_id="order-123",
                amount=Decimal("50.00"),
            )


class TestTransformedTransaction:
    def test_valid_transformed_transaction(self):
        transformed = TransformedTransaction(
            order_id="order-123",
            amount=Decimal("100.00"),
            currency="USD",
            tax_amount=Decimal("8.50"),
            total_amount=Decimal("108.50"),
            is_fraud_flagged=False,
            customer_id="cust_001",
            product_category="electronics",
            transaction_timestamp=datetime.now(timezone.utc),
            batch_id="batch_001",
        )
        assert transformed.total_amount == Decimal("108.50")
        assert transformed.is_fraud_flagged is False

    def test_fraud_flagged_transaction(self):
        transformed = TransformedTransaction(
            order_id="fraud-order",
            amount=Decimal("5000.00"),
            currency="USD",
            tax_amount=Decimal("425.00"),
            total_amount=Decimal("5425.00"),
            is_fraud_flagged=True,
            customer_id="cust_suspect",
            product_category="electronics",
            transaction_timestamp=datetime.now(timezone.utc),
            batch_id="batch_002",
        )
        assert transformed.is_fraud_flagged is True

    def test_model_dump_json_serialization(self):
        transformed = TransformedTransaction(
            order_id="order-456",
            amount=Decimal("250.00"),
            currency="EUR",
            tax_amount=Decimal("21.25"),
            total_amount=Decimal("271.25"),
            is_fraud_flagged=False,
            customer_id="cust_002",
            product_category="clothing",
            transaction_timestamp=datetime.now(timezone.utc),
            batch_id="batch_003",
        )
        data = transformed.model_dump(mode="json")
        assert "order_id" in data
        assert data["order_id"] == "order-456"
