"""Initial schema

Revision ID: 001
Revises:
Create Date: 2024-01-15

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.String(36), nullable=False),
        sa.Column("amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("tax_amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("total_amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("is_fraud_flagged", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("customer_id", sa.String(50), nullable=False),
        sa.Column("product_category", sa.String(50), nullable=False),
        sa.Column("transaction_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("batch_id", sa.String(50), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("order_id"),
    )

    op.create_index("idx_transactions_timestamp", "transactions", ["transaction_timestamp"])
    op.create_index("idx_transactions_fraud", "transactions", ["is_fraud_flagged"])
    op.create_index("idx_transactions_customer", "transactions", ["customer_id"])
    op.create_index("idx_transactions_batch", "transactions", ["batch_id"])


def downgrade() -> None:
    op.drop_index("idx_transactions_batch", table_name="transactions")
    op.drop_index("idx_transactions_customer", table_name="transactions")
    op.drop_index("idx_transactions_fraud", table_name="transactions")
    op.drop_index("idx_transactions_timestamp", table_name="transactions")
    op.drop_table("transactions")
