import pandas as pd
from decimal import Decimal, ROUND_HALF_UP


class TaxCalculator:
    DEFAULT_TAX_RATE = Decimal("0.085")

    def __init__(self, tax_rate: Decimal | None = None):
        self.tax_rate = tax_rate or self.DEFAULT_TAX_RATE

    def calculate_tax(self, amount: Decimal) -> Decimal:
        tax = amount * self.tax_rate
        return tax.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def calculate_total(self, amount: Decimal) -> tuple[Decimal, Decimal]:
        tax = self.calculate_tax(amount)
        total = amount + tax
        return tax, total

    def calculate_tax_batch(self, amounts: list[Decimal]) -> list[Decimal]:
        """Vectorized tax calculation for batch processing."""
        if not amounts:
            return []
        
        # Convert to pandas Series for vectorized operations
        amount_series = pd.Series([float(amount) for amount in amounts])
        tax_rate_float = float(self.tax_rate)
        
        # Vectorized tax calculation
        tax_series = amount_series * tax_rate_float
        
        # Convert back to Decimal with proper rounding
        return [Decimal(str(round(tax, 2))) for tax in tax_series.tolist()]

    def calculate_total_batch(self, amounts: list[Decimal]) -> tuple[list[Decimal], list[Decimal]]:
        """Vectorized total calculation for batch processing."""
        if not amounts:
            return [], []
        
        # Calculate taxes
        taxes = self.calculate_tax_batch(amounts)
        
        # Calculate totals
        totals = [amount + tax for amount, tax in zip(amounts, taxes)]
        
        return taxes, totals
