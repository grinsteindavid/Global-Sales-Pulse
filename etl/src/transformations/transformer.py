import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator

from .fraud_detection import FraudDetector
from .tax_calculator import TaxCalculator

logger = logging.getLogger(__name__)


class RawTransaction(BaseModel):
    order_id: str
    amount: Decimal
    currency: str = Field(max_length=3)
    timestamp: datetime
    customer_id: str
    product_category: str

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class TransformedTransaction(BaseModel):
    order_id: str
    amount: Decimal
    currency: str
    tax_amount: Decimal
    total_amount: Decimal
    is_fraud_flagged: bool
    customer_id: str
    product_category: str
    transaction_timestamp: datetime
    batch_id: str


class TransactionTransformer:
    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        self.tax_calculator = TaxCalculator()
        self.fraud_detector = FraudDetector()

    def transform_single(self, raw: dict[str, Any]) -> TransformedTransaction | None:
        try:
            validated = RawTransaction(**raw)
        except Exception as e:
            logger.error(f"Validation failed for {raw.get('order_id', 'unknown')}: {e}")
            return None

        tax_amount, total_amount = self.tax_calculator.calculate_total(validated.amount)
        is_fraud = self.fraud_detector.is_fraudulent(validated.amount)

        timestamp = validated.timestamp
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        return TransformedTransaction(
            order_id=validated.order_id,
            amount=validated.amount,
            currency=validated.currency,
            tax_amount=tax_amount,
            total_amount=total_amount,
            is_fraud_flagged=is_fraud,
            customer_id=validated.customer_id,
            product_category=validated.product_category,
            transaction_timestamp=timestamp,
            batch_id=self.batch_id,
        )

    def transform_batch(
        self, raw_messages: list[dict[str, Any]]
    ) -> tuple[list[TransformedTransaction], list[dict[str, Any]]]:
        transformed: list[TransformedTransaction] = []
        failed: list[dict[str, Any]] = []

        for raw in raw_messages:
            result = self.transform_single(raw)
            if result:
                transformed.append(result)
            else:
                failed.append(raw)

        logger.info(f"Transformed {len(transformed)} messages, {len(failed)} failed")
        return transformed, failed
