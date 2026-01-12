import logging
import pandas as pd
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
        if not raw_messages:
            return [], []

        # Try pandas-based batch processing first
        try:
            return self._transform_batch_pandas(raw_messages)
        except Exception as e:
            logger.warning(f"Pandas batch processing failed, falling back to sequential: {e}")
            # Fallback to original sequential processing
            return self._transform_batch_sequential(raw_messages)

    def _transform_batch_pandas(
        self, raw_messages: list[dict[str, Any]]
    ) -> tuple[list[TransformedTransaction], list[dict[str, Any]]]:
        """Pandas-based vectorized batch processing."""
        # Create DataFrame from raw messages
        df = pd.DataFrame(raw_messages)
        
        # Validate and filter valid records
        validated_df, failed_records = self._validate_dataframe(df)
        
        if validated_df.empty:
            logger.warning(f"No valid records in batch of {len(raw_messages)}")
            return [], failed_records

        # Apply vectorized transformations
        transformed_df = self._transform_dataframe(validated_df)
        
        # Convert back to Pydantic models
        transformed = self._dataframe_to_models(transformed_df)
        
        logger.info(f"Transformed {len(transformed)} messages, {len(failed_records)} failed")
        return transformed, failed_records

    def _transform_batch_sequential(
        self, raw_messages: list[dict[str, Any]]
    ) -> tuple[list[TransformedTransaction], list[dict[str, Any]]]:
        """Original sequential processing as fallback."""
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

    def _validate_dataframe(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
        """Validate DataFrame and separate valid/invalid records."""
        valid_records = []
        failed_records = []
        
        for idx, row in df.iterrows():
            try:
                # Validate using Pydantic model
                validated = RawTransaction(**row.to_dict())
                valid_records.append(validated.model_dump())
            except Exception as e:
                logger.error(f"Validation failed for record {idx}: {e}")
                failed_records.append(row.to_dict())
        
        valid_df = pd.DataFrame(valid_records) if valid_records else pd.DataFrame()
        return valid_df, failed_records

    def _transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply vectorized transformations to DataFrame."""
        # Convert amounts to Decimal for processing
        amounts = [Decimal(str(amount)) for amount in df['amount']]
        
        # Vectorized fraud detection
        fraud_flags = self.fraud_detector.detect_fraud_batch(amounts)
        
        # Vectorized tax calculation
        taxes, totals = self.tax_calculator.calculate_total_batch(amounts)
        
        # Add transformed columns
        df = df.copy()
        df['tax_amount'] = taxes
        df['total_amount'] = totals
        df['is_fraud_flagged'] = fraud_flags
        df['batch_id'] = self.batch_id
        df['processed_at'] = datetime.now(timezone.utc)
        
        # Normalize timestamps to UTC
        df['transaction_timestamp'] = df['timestamp'].apply(
            lambda ts: ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
        )
        
        return df

    def _dataframe_to_models(self, df: pd.DataFrame) -> list[TransformedTransaction]:
        """Convert DataFrame back to Pydantic models."""
        transformed = []
        
        for _, row in df.iterrows():
            try:
                model = TransformedTransaction(
                    order_id=row['order_id'],
                    amount=row['amount'],
                    currency=row['currency'],
                    tax_amount=row['tax_amount'],
                    total_amount=row['total_amount'],
                    is_fraud_flagged=row['is_fraud_flagged'],
                    customer_id=row['customer_id'],
                    product_category=row['product_category'],
                    transaction_timestamp=row['transaction_timestamp'],
                    batch_id=row['batch_id'],
                )
                transformed.append(model)
            except Exception as e:
                logger.error(f"Failed to convert row to model: {e}")
        
        return transformed
