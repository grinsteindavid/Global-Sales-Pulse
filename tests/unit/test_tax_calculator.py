from decimal import Decimal

import pytest

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "etl", "src"))

from transformations.tax_calculator import TaxCalculator


class TestTaxCalculator:
    def test_calculate_tax_default_rate(self):
        calculator = TaxCalculator()
        amount = Decimal("100.00")
        tax = calculator.calculate_tax(amount)
        assert tax == Decimal("8.50")

    def test_calculate_tax_custom_rate(self):
        calculator = TaxCalculator(tax_rate=Decimal("0.10"))
        amount = Decimal("100.00")
        tax = calculator.calculate_tax(amount)
        assert tax == Decimal("10.00")

    def test_calculate_tax_rounds_correctly(self):
        calculator = TaxCalculator()
        amount = Decimal("99.99")
        tax = calculator.calculate_tax(amount)
        assert tax == Decimal("8.50")

    def test_calculate_total(self):
        calculator = TaxCalculator()
        amount = Decimal("100.00")
        tax, total = calculator.calculate_total(amount)
        assert tax == Decimal("8.50")
        assert total == Decimal("108.50")

    def test_calculate_tax_zero_amount(self):
        calculator = TaxCalculator()
        amount = Decimal("0.00")
        tax = calculator.calculate_tax(amount)
        assert tax == Decimal("0.00")
