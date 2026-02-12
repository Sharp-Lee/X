"""Backward compatibility shim. Import from core.strategy.msr instead."""

from core.strategy.msr.generator import (
    MsrStrategy as SignalGenerator,
    ProcessKlineResult,
)
from core.strategy.msr.level_manager import LevelManager, _is_nan
from core.strategy.protocol import (
    SignalCallback,
    SaveSignalCallback,
    SaveStreakCallback,
    LoadStreaksCallback,
    LoadActiveSignalsCallback,
)

__all__ = [
    "SignalGenerator",
    "LevelManager",
    "ProcessKlineResult",
    "_is_nan",
    "SignalCallback",
    "SaveSignalCallback",
    "SaveStreakCallback",
    "LoadStreaksCallback",
    "LoadActiveSignalsCallback",
]
