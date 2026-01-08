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
