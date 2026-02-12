"""EMA Crossover strategy configuration and signal models."""

from decimal import Decimal

from pydantic import BaseModel

from core.strategy.base_signal import BaseSignalRecord

EMA_CROSSOVER_STRATEGY_NAME = "ema_crossover"


class EmaCrossoverConfig(BaseModel):
    """Configuration for the EMA Crossover strategy."""

    fast_period: int = 20
    slow_period: int = 50
    atr_period: int = 9

    # TP/SL multipliers (based on ATR)
    tp_atr_mult: Decimal = Decimal("2.0")
    sl_atr_mult: Decimal = Decimal("4.0")


class EmaSignalRecord(BaseSignalRecord):
    """EMA Crossover signal with strategy-specific fields."""

    strategy: str = EMA_CROSSOVER_STRATEGY_NAME

    # EMA values at signal time
    ema_fast: Decimal = Decimal("0")
    ema_slow: Decimal = Decimal("0")

    # ATR at signal time
    atr_at_signal: Decimal = Decimal("0")
