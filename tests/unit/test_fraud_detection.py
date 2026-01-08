from decimal import Decimal

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "etl", "src"))

from transformations.fraud_detection import FraudDetector


class TestFraudDetector:
    def test_is_fraudulent_above_threshold(self):
        detector = FraudDetector()
        assert detector.is_fraudulent(Decimal("1001.00")) is True

    def test_is_fraudulent_at_threshold(self):
        detector = FraudDetector()
        assert detector.is_fraudulent(Decimal("1000.00")) is False

    def test_is_fraudulent_below_threshold(self):
        detector = FraudDetector()
        assert detector.is_fraudulent(Decimal("999.99")) is False

    def test_custom_threshold(self):
        detector = FraudDetector(threshold=Decimal("500.00"))
        assert detector.is_fraudulent(Decimal("501.00")) is True
        assert detector.is_fraudulent(Decimal("499.00")) is False
