import uuid
from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field


class Transaction(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    amount: Decimal = Field(ge=0, decimal_places=2)
    currency: str = Field(default="USD", max_length=3)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    customer_id: str
    product_category: str

    def to_json(self) -> str:
        return self.model_dump_json()
