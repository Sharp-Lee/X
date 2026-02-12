"""MSR Retest Capture strategy package.

Importing this package triggers strategy registration via the
@register_strategy decorator on MsrStrategy.
"""

from core.strategy.msr.generator import MsrStrategy, SignalGenerator, ProcessKlineResult
from core.strategy.msr.level_manager import LevelManager, _is_nan
from core.strategy.msr.models import MsrConfig, MsrSignalRecord, MSR_STRATEGY_NAME

__all__ = [
    "MsrStrategy",
    "SignalGenerator",
    "ProcessKlineResult",
    "LevelManager",
    "MsrConfig",
    "MsrSignalRecord",
    "MSR_STRATEGY_NAME",
    "_is_nan",
]
