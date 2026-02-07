"""Converters between hot path (fast) and cold path (Pydantic) models.

Hot path models use:
- float instead of Decimal
- Unix timestamps (float) instead of datetime
- str literals instead of Enum

Cold path models use:
- Decimal for precision
- datetime for time handling
- Enum for type safety

Conversion functions handle the transformation between these representations.
"""

from datetime import datetime, timezone
from decimal import Decimal

from core.models.fast import (
    FastKline,
    FastTrade,
    FastSignal,
    FastKlineBuffer,
    DIRECTION_LONG,
    DIRECTION_SHORT,
)
from core.models.kline import Kline, KlineBuffer
from core.models.signal import AggTrade, Direction, Outcome, SignalRecord


# =============================================================================
# Timestamp conversion helpers
# =============================================================================

def datetime_to_timestamp(dt: datetime) -> float:
    """Convert datetime to Unix timestamp."""
    return dt.timestamp()


def timestamp_to_datetime(ts: float) -> datetime:
    """Convert Unix timestamp to UTC datetime."""
    return datetime.fromtimestamp(ts, tz=timezone.utc)


# =============================================================================
# Kline conversions
# =============================================================================

def kline_to_fast(kline: Kline) -> FastKline:
    """Convert Pydantic Kline to FastKline.

    Args:
        kline: Pydantic Kline model

    Returns:
        FastKline dataclass
    """
    return FastKline(
        symbol=kline.symbol,
        timeframe=kline.timeframe,
        timestamp=datetime_to_timestamp(kline.timestamp),
        open=float(kline.open),
        high=float(kline.high),
        low=float(kline.low),
        close=float(kline.close),
        volume=float(kline.volume),
        is_closed=kline.is_closed,
    )


def fast_to_kline(fast: FastKline) -> Kline:
    """Convert FastKline to Pydantic Kline.

    Args:
        fast: FastKline dataclass

    Returns:
        Pydantic Kline model
    """
    return Kline(
        symbol=fast.symbol,
        timeframe=fast.timeframe,
        timestamp=timestamp_to_datetime(fast.timestamp),
        open=Decimal(str(fast.open)),
        high=Decimal(str(fast.high)),
        low=Decimal(str(fast.low)),
        close=Decimal(str(fast.close)),
        volume=Decimal(str(fast.volume)),
        is_closed=fast.is_closed,
    )


def kline_buffer_to_fast(buffer: KlineBuffer) -> FastKlineBuffer:
    """Convert Pydantic KlineBuffer to FastKlineBuffer.

    Args:
        buffer: Pydantic KlineBuffer model

    Returns:
        FastKlineBuffer dataclass
    """
    fast_buffer = FastKlineBuffer(
        symbol=buffer.symbol,
        timeframe=buffer.timeframe,
        max_size=buffer.max_size,
    )
    for kline in buffer.klines:
        fast_buffer.add(kline_to_fast(kline))
    return fast_buffer


def fast_to_kline_buffer(fast: FastKlineBuffer) -> KlineBuffer:
    """Convert FastKlineBuffer to Pydantic KlineBuffer.

    Args:
        fast: FastKlineBuffer dataclass

    Returns:
        Pydantic KlineBuffer model
    """
    buffer = KlineBuffer(
        symbol=fast.symbol,
        timeframe=fast.timeframe,
        max_size=fast.max_size,
    )
    for kline in fast.klines:
        buffer.add(fast_to_kline(kline))
    return buffer


# =============================================================================
# Trade conversions
# =============================================================================

def aggtrade_to_fast(trade: AggTrade) -> FastTrade:
    """Convert Pydantic AggTrade to FastTrade.

    Args:
        trade: Pydantic AggTrade model

    Returns:
        FastTrade dataclass
    """
    return FastTrade(
        symbol=trade.symbol,
        agg_trade_id=trade.agg_trade_id,
        price=float(trade.price),
        quantity=float(trade.quantity),
        timestamp=datetime_to_timestamp(trade.timestamp),
        is_buyer_maker=trade.is_buyer_maker,
    )


def fast_to_aggtrade(fast: FastTrade) -> AggTrade:
    """Convert FastTrade to Pydantic AggTrade.

    Args:
        fast: FastTrade dataclass

    Returns:
        Pydantic AggTrade model
    """
    return AggTrade(
        symbol=fast.symbol,
        agg_trade_id=fast.agg_trade_id,
        price=Decimal(str(fast.price)),
        quantity=Decimal(str(fast.quantity)),
        timestamp=timestamp_to_datetime(fast.timestamp),
        is_buyer_maker=fast.is_buyer_maker,
    )


# =============================================================================
# Signal conversions
# =============================================================================

def _direction_to_int(direction: Direction) -> int:
    """Convert Direction enum to int."""
    return DIRECTION_LONG if direction == Direction.LONG else DIRECTION_SHORT


def _int_to_direction(value: int) -> Direction:
    """Convert int to Direction enum."""
    return Direction.LONG if value == DIRECTION_LONG else Direction.SHORT


def _outcome_to_str(outcome: Outcome) -> str:
    """Convert Outcome enum to string."""
    return outcome.value


def _str_to_outcome(value: str) -> Outcome:
    """Convert string to Outcome enum."""
    return Outcome(value)


def signal_to_fast(signal: SignalRecord) -> FastSignal:
    """Convert Pydantic SignalRecord to FastSignal.

    Args:
        signal: Pydantic SignalRecord model

    Returns:
        FastSignal dataclass
    """
    return FastSignal(
        id=signal.id,
        symbol=signal.symbol,
        timeframe=signal.timeframe,
        signal_time=datetime_to_timestamp(signal.signal_time),
        direction=_direction_to_int(signal.direction),
        entry_price=float(signal.entry_price),
        tp_price=float(signal.tp_price),
        sl_price=float(signal.sl_price),
        atr_at_signal=float(signal.atr_at_signal),
        max_atr=float(signal.max_atr),
        streak_at_signal=signal.streak_at_signal,
        mae_ratio=float(signal.mae_ratio),
        mfe_ratio=float(signal.mfe_ratio),
        outcome=_outcome_to_str(signal.outcome),
        outcome_time=(
            datetime_to_timestamp(signal.outcome_time)
            if signal.outcome_time else None
        ),
        outcome_price=(
            float(signal.outcome_price)
            if signal.outcome_price else None
        ),
    )


def fast_to_signal(fast: FastSignal) -> SignalRecord:
    """Convert FastSignal to Pydantic SignalRecord.

    Args:
        fast: FastSignal dataclass

    Returns:
        Pydantic SignalRecord model
    """
    return SignalRecord(
        id=fast.id,
        symbol=fast.symbol,
        timeframe=fast.timeframe,
        signal_time=timestamp_to_datetime(fast.signal_time),
        direction=_int_to_direction(fast.direction),
        entry_price=Decimal(str(fast.entry_price)),
        tp_price=Decimal(str(fast.tp_price)),
        sl_price=Decimal(str(fast.sl_price)),
        atr_at_signal=Decimal(str(fast.atr_at_signal)),
        max_atr=Decimal(str(fast.max_atr)),
        streak_at_signal=fast.streak_at_signal,
        mae_ratio=Decimal(str(fast.mae_ratio)),
        mfe_ratio=Decimal(str(fast.mfe_ratio)),
        outcome=_str_to_outcome(fast.outcome),
        outcome_time=(
            timestamp_to_datetime(fast.outcome_time)
            if fast.outcome_time else None
        ),
        outcome_price=(
            Decimal(str(fast.outcome_price))
            if fast.outcome_price else None
        ),
    )


# =============================================================================
# Batch conversions
# =============================================================================

def klines_to_fast(klines: list[Kline]) -> list[FastKline]:
    """Convert a list of Pydantic Klines to FastKlines."""
    return [kline_to_fast(k) for k in klines]


def fast_to_klines(fast_klines: list[FastKline]) -> list[Kline]:
    """Convert a list of FastKlines to Pydantic Klines."""
    return [fast_to_kline(k) for k in fast_klines]


def trades_to_fast(trades: list[AggTrade]) -> list[FastTrade]:
    """Convert a list of Pydantic AggTrades to FastTrades."""
    return [aggtrade_to_fast(t) for t in trades]


def fast_to_trades(fast_trades: list[FastTrade]) -> list[AggTrade]:
    """Convert a list of FastTrades to Pydantic AggTrades."""
    return [fast_to_aggtrade(t) for t in fast_trades]


def signals_to_fast(signals: list[SignalRecord]) -> list[FastSignal]:
    """Convert a list of Pydantic SignalRecords to FastSignals."""
    return [signal_to_fast(s) for s in signals]


def fast_to_signals(fast_signals: list[FastSignal]) -> list[SignalRecord]:
    """Convert a list of FastSignals to Pydantic SignalRecords."""
    return [fast_to_signal(s) for s in fast_signals]
