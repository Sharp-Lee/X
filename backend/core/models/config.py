"""Trading configuration models."""

from decimal import Decimal
from pydantic import BaseModel


class StrategyConfig(BaseModel):
    """Strategy configuration parameters."""

    # Indicator periods
    ema_period: int = 50
    fib_period: int = 9
    atr_period: int = 9

    # Fibonacci levels for support/resistance
    fib_levels: list[Decimal] = [
        Decimal("0.382"),
        Decimal("0.5"),
        Decimal("0.618"),
    ]

    # TP/SL multipliers (based on ATR)
    tp_atr_mult: Decimal = Decimal("2.0")
    sl_atr_mult: Decimal = Decimal("8.84")  # 2 * 4.42

    # Risk management (reserved for future position sizing, not used by SignalGenerator)
    max_risk_percent: Decimal = Decimal("2.53")  # Max risk per trade as % of equity

    # Tolerance for price touching levels (as percentage)
    touch_tolerance: Decimal = Decimal("0.001")  # 0.1%


class SymbolConfig(BaseModel):
    """Per-symbol configuration."""

    symbol: str
    timeframe: str = "5m"
    enabled: bool = True
    strategy: StrategyConfig = StrategyConfig()

    # Price precision for the symbol
    price_precision: int = 2
    quantity_precision: int = 3
