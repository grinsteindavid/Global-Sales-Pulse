import pandas as pd
from decimal import Decimal


class FraudDetector:
    DEFAULT_THRESHOLD = Decimal("1000.00")

    def __init__(self, threshold: Decimal | None = None):
        self.threshold = threshold or self.DEFAULT_THRESHOLD

    def is_fraudulent(self, amount: Decimal) -> bool:
        return amount > self.threshold

    def detect_fraud_batch(self, amounts: list[Decimal]) -> list[bool]:
        """Vectorized fraud detection for batch processing."""
        if not amounts:
            return []
        
        # Convert to pandas Series for vectorized operations
        amount_series = pd.Series([float(amount) for amount in amounts])
        threshold_float = float(self.threshold)
        
        # Vectorized comparison
        fraud_flags = amount_series > threshold_float
        
        return fraud_flags.tolist()
