from datetime import datetime, timezone
from decimal import Decimal

import pytest

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "etl", "src"))

from loaders.postgres_loader import PostgresLoader
from models import Transaction
from transformations.transformer import TransformedTransaction


class TestPostgresLoader:
    @pytest.fixture
    def sample_transactions(self):
        return [
            TransformedTransaction(
                order_id="test-order-001",
                amount=Decimal("100.00"),
                currency="USD",
                tax_amount=Decimal("8.50"),
                total_amount=Decimal("108.50"),
                is_fraud_flagged=False,
                customer_id="cust_12345",
                product_category="electronics",
                transaction_timestamp=datetime.now(timezone.utc),
                batch_id="test_batch",
            ),
            TransformedTransaction(
                order_id="test-order-002",
                amount=Decimal("1500.00"),
                currency="EUR",
                tax_amount=Decimal("127.50"),
                total_amount=Decimal("1627.50"),
                is_fraud_flagged=True,
                customer_id="cust_67890",
                product_category="clothing",
                transaction_timestamp=datetime.now(timezone.utc),
                batch_id="test_batch",
            ),
        ]

    def test_load_batch(self, session, sample_transactions):
        loader = PostgresLoader(session)
        loaded_count = loader.load_batch(sample_transactions)

        assert loaded_count >= 0

        results = session.query(Transaction).all()
        assert len(results) == 2

    def test_load_empty_batch(self, session):
        loader = PostgresLoader(session)
        loaded_count = loader.load_batch([])
        assert loaded_count == 0

    def test_load_duplicate_order_id(self, session, sample_transactions):
        loader = PostgresLoader(session)

        loader.load_batch(sample_transactions)
        loaded_count = loader.load_batch(sample_transactions)

        results = session.query(Transaction).all()
        assert len(results) == 2
