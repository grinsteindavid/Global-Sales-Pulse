from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    tax_amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    total_amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    is_fraud_flagged: Mapped[bool] = mapped_column(Boolean, default=False)
    customer_id: Mapped[str] = mapped_column(String(50), nullable=False)
    product_category: Mapped[str] = mapped_column(String(50), nullable=False)
    transaction_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    batch_id: Mapped[str] = mapped_column(String(50), nullable=False)

    __table_args__ = (
        Index("idx_transactions_timestamp", "transaction_timestamp"),
        Index("idx_transactions_fraud", "is_fraud_flagged"),
        Index("idx_transactions_customer", "customer_id"),
        Index("idx_transactions_batch", "batch_id"),
    )

    def __repr__(self) -> str:
        return f"<Transaction(order_id={self.order_id}, amount={self.amount})>"
