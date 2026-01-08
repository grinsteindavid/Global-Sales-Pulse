"""End-to-end tests for the ETL pipeline.

Tests the full flow: Raw message → Transform → Load to database.
These tests mock Kafka but use a real database connection.
"""
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

etl_src = Path(__file__).parent.parent.parent / "etl" / "src"
sys.path.insert(0, str(etl_src))

from transformations.transformer import TransactionTransformer, TransformedTransaction
from transformations.tax_calculator import TaxCalculator
from transformations.fraud_detection import FraudDetector
from models.transaction import Transaction
from models.base import Base


class PostgresLoader:
    """Simplified loader for e2e tests (mirrors etl/src/loaders/postgres_loader.py)."""
    
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


def create_raw_message(
    order_id: str = None,
    amount: float = 100.00,
    currency: str = "USD",
    customer_id: str = "cust_12345",
    product_category: str = "electronics",
) -> dict:
    """Helper to create a raw Kafka message dict."""
    import uuid
    return {
        "order_id": order_id or str(uuid.uuid4()),
        "amount": str(amount),
        "currency": currency,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer_id,
        "product_category": product_category,
    }


class TestE2EPipeline:
    """End-to-end tests simulating the full ETL pipeline."""

    @pytest.fixture
    def batch_id(self):
        return f"e2e_test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    def test_single_transaction_flow(self, session, batch_id):
        """Test: Raw message → Transform → Load → Verify in DB."""
        raw_message = create_raw_message(
            order_id="e2e-order-001",
            amount=150.00,
            product_category="electronics",
        )

        transformer = TransactionTransformer(batch_id=batch_id)
        result = transformer.transform_single(raw_message)

        assert result is not None
        assert result.order_id == "e2e-order-001"
        assert result.tax_amount == Decimal("12.75")
        assert result.total_amount == Decimal("162.75")
        assert result.is_fraud_flagged is False

        loader = PostgresLoader(session)
        loaded = loader.load_batch([result])
        assert loaded >= 1

        db_record = session.query(Transaction).filter_by(order_id="e2e-order-001").first()
        assert db_record is not None
        assert float(db_record.amount) == 150.00
        assert db_record.batch_id == batch_id

    def test_batch_transaction_flow(self, session, batch_id):
        """Test: Multiple raw messages → Transform batch → Load batch."""
        raw_messages = [
            create_raw_message(order_id="e2e-batch-001", amount=50.00),
            create_raw_message(order_id="e2e-batch-002", amount=200.00),
            create_raw_message(order_id="e2e-batch-003", amount=75.50),
        ]

        transformer = TransactionTransformer(batch_id=batch_id)
        transformed, failed = transformer.transform_batch(raw_messages)

        assert len(transformed) == 3
        assert len(failed) == 0

        loader = PostgresLoader(session)
        loaded = loader.load_batch(transformed)
        assert loaded >= 3

        records = session.query(Transaction).filter(
            Transaction.batch_id == batch_id
        ).all()
        assert len(records) == 3

    def test_fraud_detection_in_pipeline(self, session, batch_id):
        """Test: High-value transaction gets fraud-flagged through pipeline."""
        raw_message = create_raw_message(
            order_id="e2e-fraud-001",
            amount=1500.00,
            customer_id="suspicious_customer",
        )

        transformer = TransactionTransformer(batch_id=batch_id)
        result = transformer.transform_single(raw_message)

        assert result.is_fraud_flagged is True

        loader = PostgresLoader(session)
        loader.load_batch([result])

        db_record = session.query(Transaction).filter_by(order_id="e2e-fraud-001").first()
        assert db_record is not None
        assert db_record.is_fraud_flagged is True

    def test_mixed_valid_invalid_batch(self, session, batch_id):
        """Test: Batch with valid and invalid messages."""
        raw_messages = [
            create_raw_message(order_id="e2e-valid-001", amount=100.00),
            {"invalid": "message", "missing": "fields"},
            create_raw_message(order_id="e2e-valid-002", amount=200.00),
            {"order_id": "incomplete"},
        ]

        transformer = TransactionTransformer(batch_id=batch_id)
        transformed, failed = transformer.transform_batch(raw_messages)

        assert len(transformed) == 2
        assert len(failed) == 2

        loader = PostgresLoader(session)
        loaded = loader.load_batch(transformed)
        assert loaded >= 2

        valid_records = session.query(Transaction).filter(
            Transaction.order_id.in_(["e2e-valid-001", "e2e-valid-002"])
        ).all()
        assert len(valid_records) == 2

    def test_idempotency_duplicate_order_ids(self, session, batch_id):
        """Test: Duplicate order_ids are handled gracefully (upsert)."""
        raw_message = create_raw_message(
            order_id="e2e-duplicate-001",
            amount=100.00,
        )

        transformer = TransactionTransformer(batch_id=batch_id)
        result = transformer.transform_single(raw_message)

        loader = PostgresLoader(session)
        loader.load_batch([result])
        loader.load_batch([result])

        count = session.query(Transaction).filter_by(
            order_id="e2e-duplicate-001"
        ).count()
        assert count == 1

    def test_different_currencies(self, session, batch_id):
        """Test: Transactions with different currencies."""
        currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]
        raw_messages = [
            create_raw_message(
                order_id=f"e2e-currency-{i}",
                amount=100.00,
                currency=curr,
            )
            for i, curr in enumerate(currencies)
        ]

        transformer = TransactionTransformer(batch_id=batch_id)
        transformed, _ = transformer.transform_batch(raw_messages)

        loader = PostgresLoader(session)
        loader.load_batch(transformed)

        for i, curr in enumerate(currencies):
            record = session.query(Transaction).filter_by(
                order_id=f"e2e-currency-{i}"
            ).first()
            assert record is not None
            assert record.currency == curr

    def test_all_product_categories(self, session, batch_id):
        """Test: Transactions across all product categories."""
        categories = [
            "electronics", "clothing", "home_garden", "sports",
            "books", "toys", "automotive", "health_beauty",
        ]
        raw_messages = [
            create_raw_message(
                order_id=f"e2e-category-{i}",
                amount=50.00 * (i + 1),
                product_category=cat,
            )
            for i, cat in enumerate(categories)
        ]

        transformer = TransactionTransformer(batch_id=batch_id)
        transformed, _ = transformer.transform_batch(raw_messages)

        loader = PostgresLoader(session)
        loader.load_batch(transformed)

        for i, cat in enumerate(categories):
            record = session.query(Transaction).filter_by(
                order_id=f"e2e-category-{i}"
            ).first()
            assert record is not None
            assert record.product_category == cat

    def test_tax_calculation_accuracy(self, session, batch_id):
        """Test: Verify tax calculations are accurate at 8.5% using ROUND_HALF_UP."""
        from decimal import ROUND_HALF_UP
        
        test_amounts = [100.00, 999.99, 1.00, 0.01, 12345.67]
        
        for i, amount in enumerate(test_amounts):
            raw_message = create_raw_message(
                order_id=f"e2e-tax-{i}",
                amount=amount,
            )
            transformer = TransactionTransformer(batch_id=batch_id)
            result = transformer.transform_single(raw_message)

            tax = Decimal(str(amount)) * Decimal("0.085")
            expected_tax = tax.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            assert result.tax_amount == expected_tax

            loader = PostgresLoader(session)
            loader.load_batch([result])

    def test_empty_batch_handling(self, session, batch_id):
        """Test: Empty batch doesn't cause errors."""
        transformer = TransactionTransformer(batch_id=batch_id)
        transformed, failed = transformer.transform_batch([])

        assert len(transformed) == 0
        assert len(failed) == 0

        loader = PostgresLoader(session)
        loaded = loader.load_batch([])
        assert loaded == 0
