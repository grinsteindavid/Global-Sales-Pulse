from decimal import Decimal


class FraudDetector:
    DEFAULT_THRESHOLD = Decimal("1000.00")

    def __init__(self, threshold: Decimal | None = None):
        self.threshold = threshold or self.DEFAULT_THRESHOLD

    def is_fraudulent(self, amount: Decimal) -> bool:
        return amount > self.threshold
