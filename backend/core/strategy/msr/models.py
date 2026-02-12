"""MSR strategy configuration and signal models.

Re-exports StrategyConfig from core.models.config as MsrConfig
for strategy-specific naming while maintaining backward compatibility.

Also defines MsrSignalRecord with MSR-specific signal fields.
"""

from decimal import Decimal

from core.models.config import StrategyConfig
from core.strategy.base_signal import BaseSignalRecord

# MsrConfig is an alias for StrategyConfig.
# All MSR-specific parameters (ema_period, fib_period, atr_period,
# tp_atr_mult, sl_atr_mult, touch_tolerance) are already defined there.
MsrConfig = StrategyConfig

MSR_STRATEGY_NAME = "msr_retest_capture"


class MsrSignalRecord(BaseSignalRecord):
    """MSR Retest Capture signal with strategy-specific fields."""

    strategy: str = MSR_STRATEGY_NAME

    # MSR-specific fields
    atr_at_signal: Decimal = Decimal("0")  # ATR value when signal was generated
    max_atr: Decimal = Decimal("0")  # Maximum ATR during signal lifetime
    streak_at_signal: int = 0  # Positive = win streak, negative = loss streak
