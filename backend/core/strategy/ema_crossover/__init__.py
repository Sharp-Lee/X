"""EMA Crossover strategy package.

Importing this package triggers strategy registration via the
@register_strategy decorator on EmaCrossoverStrategy.
"""

from core.strategy.ema_crossover.generator import EmaCrossoverStrategy
from core.strategy.ema_crossover.models import (
    EmaCrossoverConfig,
    EmaSignalRecord,
    EMA_CROSSOVER_STRATEGY_NAME,
)

__all__ = [
    "EmaCrossoverStrategy",
    "EmaCrossoverConfig",
    "EmaSignalRecord",
    "EMA_CROSSOVER_STRATEGY_NAME",
]
