from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Index, Integer, Numeric, String

from .base import Base


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(36), unique=True, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    currency = Column(String(3), nullable=False)
    tax_amount = Column(Numeric(12, 2), nullable=False)
    total_amount = Column(Numeric(12, 2), nullable=False)
    is_fraud_flagged = Column(Boolean, default=False)
    customer_id = Column(String(50), nullable=False)
    product_category = Column(String(50), nullable=False)
    transaction_timestamp = Column(DateTime(timezone=True), nullable=False)
    processed_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    batch_id = Column(String(50), nullable=False)

    __table_args__ = (
        Index("idx_transactions_timestamp", "transaction_timestamp"),
        Index("idx_transactions_fraud", "is_fraud_flagged"),
        Index("idx_transactions_customer", "customer_id"),
        Index("idx_transactions_batch", "batch_id"),
    )

    def __repr__(self) -> str:
        return f"<Transaction(order_id={self.order_id}, amount={self.amount})>"
