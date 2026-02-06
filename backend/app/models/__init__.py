"""Data models."""

from app.models.kline import Kline, KlineBuffer
from app.models.signal import (
    AggTrade,
    Direction,
    Outcome,
    SignalRecord,
    StreakTracker,
)
from app.models.config import StrategyConfig, SymbolConfig
from app.models.processing_state import ProcessingState
from app.models.fast import (
    FastKline,
    FastTrade,
    FastSignal,
    FastKlineBuffer,
    DIRECTION_LONG,
    DIRECTION_SHORT,
    generate_signal_id,
)
from app.models.converters import (
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
