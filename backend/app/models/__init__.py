"""Data models - re-exported from core.models for backwards compatibility."""

from core.models.kline import Kline, KlineBuffer
from core.models.signal import (
    AggTrade,
    Direction,
    Outcome,
    SignalRecord,
    StreakTracker,
)
from core.models.config import (
    StrategyConfig,
    SymbolConfig,
    SignalFilterConfig,
    PORTFOLIO_A,
    PORTFOLIO_B,
)
from core.models.processing_state import ProcessingState
from core.models.fast import (
    FastKline,
    FastTrade,
    FastSignal,
    FastKlineBuffer,
    DIRECTION_LONG,
    DIRECTION_SHORT,
    generate_signal_id,
)
from core.models.converters import (
    kline_to_fast,
    fast_to_kline,
    kline_buffer_to_fast,
    fast_to_kline_buffer,
    aggtrade_to_fast,
    fast_to_aggtrade,
    signal_to_fast,
    fast_to_signal,
    datetime_to_timestamp,
    timestamp_to_datetime,
)

__all__ = [
    # Cold path (Pydantic)
    "Kline",
    "KlineBuffer",
    "AggTrade",
    "Direction",
    "Outcome",
    "SignalRecord",
    "StreakTracker",
    "StrategyConfig",
    "SymbolConfig",
    "SignalFilterConfig",
    "PORTFOLIO_A",
    "PORTFOLIO_B",
    "ProcessingState",
    # Hot path (dataclass)
    "FastKline",
    "FastTrade",
    "FastSignal",
    "FastKlineBuffer",
    "DIRECTION_LONG",
    "DIRECTION_SHORT",
    "generate_signal_id",
    # Converters
    "kline_to_fast",
    "fast_to_kline",
    "kline_buffer_to_fast",
    "fast_to_kline_buffer",
    "aggtrade_to_fast",
    "fast_to_aggtrade",
    "signal_to_fast",
    "fast_to_signal",
    "datetime_to_timestamp",
    "timestamp_to_datetime",
]
