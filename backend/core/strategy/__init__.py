"""Strategy plugin system.

Public API:
- Strategy: Protocol that all strategies must implement
- ProcessResult: Standard return type from strategy processing
- register_strategy: Decorator to register a strategy class
- create_strategy: Factory function to instantiate strategies by name
- list_strategies: Discover all registered strategies
- get_strategy_class: Get strategy class by name without instantiating

Importing this package auto-registers all built-in strategies.
"""

from core.strategy.protocol import (
    Strategy,
    ProcessResult,
    SignalCallback,
    SaveSignalCallback,
    SaveStreakCallback,
    LoadStreaksCallback,
    LoadActiveSignalsCallback,
)
from core.strategy.registry import (
    register_strategy,
    create_strategy,
    list_strategies,
    get_strategy_class,
)
from core.strategy.base_signal import BaseSignalRecord
from core.strategy.signal_repo_protocol import SignalRepository as SignalRepoProtocol

# Import built-in strategies to trigger auto-registration
import core.strategy.msr  # noqa: F401
import core.strategy.ema_crossover  # noqa: F401

__all__ = [
    "Strategy",
    "ProcessResult",
    "SignalCallback",
    "SaveSignalCallback",
    "SaveStreakCallback",
    "LoadStreaksCallback",
    "LoadActiveSignalsCallback",
    "register_strategy",
    "create_strategy",
    "list_strategies",
    "get_strategy_class",
    "BaseSignalRecord",
    "SignalRepoProtocol",
]
