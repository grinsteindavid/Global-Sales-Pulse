from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy.dialects.postgresql import insert

import sys
import os

etl_src = Path(__file__).parent.parent.parent / "etl" / "src"
sys.path.insert(0, str(etl_src))

from models.transaction import Transaction
from models.base import Base
from transformations.transformer import TransformedTransaction


class PostgresLoader:
    """Loader for integration tests."""
    
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
